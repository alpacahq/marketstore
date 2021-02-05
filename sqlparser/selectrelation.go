package sqlparser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alpacahq/marketstore/v4/catalog"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type SelectRelation struct {
	ExecutableStatement
	Limit                  int
	OrderBy                []SortItem
	SelectList             []*AliasedIdentifier
	IsPrimary, IsSelectAll bool
	PrimaryTargetName      []string
	Subquery               *SelectRelation
	WherePredicate         IMSTree // Runtime predicates
	SetQuantifier          SetQuantifierEnum
	StaticPredicates       StaticPredicateGroup
}

func NewSelectRelation(disableVariableCompression bool, catalogDir *catalog.Directory) (sr *SelectRelation) {
	return &SelectRelation{ExecutableStatement: ExecutableStatement{
		DisableVariableCompression: disableVariableCompression,
		CatalogDirectory:           catalogDir,
	}}
}

func (sr *SelectRelation) Materialize() (outputColumnSeries *io.ColumnSeries, err error) {
	// Call Materialize on any child relations
	//	fmt.Println("In SelectRelation Materialize")
	var inputColumnSeries *io.ColumnSeries
	for _, node := range sr.GetChildren() {
		//		fmt.Printf("Child nodes present...")
		switch value := node.(type) {
		case Relation: // Interface type
			//fmt.Println("Subquery Interface found...")
			//			fmt.Println("Relation")
			inputColumnSeries, err = value.Materialize()
			if err != nil {
				return nil, err
			}
		case *SelectRelation:
			//			fmt.Println("*SelectRelation")
			//fmt.Println("Subquery found...")
			inputColumnSeries, err = value.Materialize()
			if err != nil {
				return nil, err
			}
		}
	}
	//	fmt.Printf("Materialize... %+v\n", sr)
	if !sr.IsPrimary {
		//		fmt.Println("Materializing subquery")
		inputColumnSeries, err = sr.Subquery.Materialize()
		if err != nil {
			return nil, err
		}
	}

	// Check for the early "always false predicate" case
	for _, sp := range sr.StaticPredicates {
		if sp.IsFalse() {
			return io.NewColumnSeries(), nil // Return an empty set
		}
	}

	/*
		Get column metadata, either from primary table or from input results
	*/
	var dsv []io.DataShape
	var key *io.TimeBucketKey
	if inputColumnSeries != nil {
		dsv = inputColumnSeries.GetDataShapes()
	} else {
		if len(sr.PrimaryTargetName) == 0 {
			return nil, fmt.Errorf("Unable to retrieve table name")
		}
		key = io.NewTimeBucketKey(sr.PrimaryTargetName[0], "Symbol/Timeframe/AttributeGroup")
		if key == nil {
			return nil, fmt.Errorf("Table name must match \"one/two/three\" for three directory levels")
		}
		dsv, err = sr.CatalogDirectory.GetDataShapes(key)
		if err != nil {
			return nil, err
		}
	}

	/*
		Validate the SELECT list
	*/
	var valid bool
	var keepList, missing []string // List of all columns needed in the output result
	if !sr.IsSelectAll {
		/*
			Set up a validator via a map of the primary data columns to the
			relation output names
		*/
		dsv = append(dsv, io.DataShape{Name: "Epoch", Type: io.INT64})
		valid, missing, keepList, _, err = SourceValidator(dsv, sr.SelectList)
		if err != nil {
			return nil, err
		}
		if !valid {
			var buffer bytes.Buffer
			for _, item := range missing {
				buffer.WriteString(item + ": ")
			}
			allMissing := buffer.String()
			buffer.Reset()
			for _, item := range dsv {
				buffer.WriteString(item.String() + ": ")
			}
			allTable := buffer.String()
			return nil, fmt.Errorf("Query columns not found in source table\n missing: %s\n have: %s\n",
				allMissing, allTable)
		}
	}
	//fmt.Println("Valid, Missing, Keeplist:", valid, missing, keepList)

	/*
		Get input results, either by query or using input results
	*/
	if inputColumnSeries != nil {
		outputColumnSeries = inputColumnSeries
	} else {
		q := planner.NewQuery(sr.CatalogDirectory)
		q.AddTargetKey(key)

		/*
			Search for time/Epoch predicates and push them down to the IO query
		*/
		if sp, ok := sr.StaticPredicates["Epoch"]; ok {
			if sp.ContentsEnum.IsSet(MINBOUND) {
				val, err := io.GetValueAsInt64(sp.min)
				if err != nil {
					return nil, fmt.Errorf("Non date predicate found for Epoch")
				}
				if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
					val += 1
				}
				q.SetStart(time.Unix(val, 0))
			}
			if sp.ContentsEnum.IsSet(MAXBOUND) {
				val, err := io.GetValueAsInt64(sp.max)
				if err != nil {
					return nil, fmt.Errorf("Non date predicate found for Epoch")
				}
				if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
					val -= 1
				}
				q.SetEnd(time.Unix(val, 0))
			}
		}

		// TODO: push down range predicates on Epoch column
		checkForPredicatesAndFunctions := func() bool {
			// First check for predicates - we don't push these down (even though we can for Epoch predicates)
			if len(sr.StaticPredicates) != 0 {
				return true
			}
			// Check for functions on the relation
			if !sr.IsSelectAll {
				for _, sl := range sr.SelectList {
					if sl.IsFunctionCall {
						return true
					}
				}
			}
			return false
		}
		if !checkForPredicatesAndFunctions() {
			if sr.Limit != 0 {
				q.SetRowLimit(io.FIRST, sr.Limit)
			}
		}

		parsed, err := q.Parse()
		if err != nil {
			return nil, err
		}
		scanner, err := executor.NewReader(parsed, sr.DisableVariableCompression, false)
		if err != nil {
			return nil, err
		}
		csm, err := scanner.Read()
		if err != nil {
			return nil, err
		}
		if len(csm) == 0 {
			return nil, fmt.Errorf("No results returned from query")
		}

		outputColumnSeries = csm[*key]
		if outputColumnSeries.Len() == 0 {
			return outputColumnSeries, nil
		}

		/*
			Evaluate all predicates on final results set
		*/
		totalLength := outputColumnSeries.Len()
		removalBitmap := make([]bool, totalLength, totalLength) // true means we ditch the value, default is keep
		for _, name := range outputColumnSeries.GetColumnNames() {
			if sp, ok := sr.StaticPredicates[name]; ok {
				i_col := outputColumnSeries.GetColumn(name)
				switch col := i_col.(type) {
				case []float32:
					if sp.ContentsEnum.IsSet(EQUALITY) {
						eqval, _ := io.GetValueAsFloat64(sp.equal)
						for i, val := range col {
							if val != float32(eqval) {
								removalBitmap[i] = true // remove
							}
						}
					}
					if sp.ContentsEnum.IsSet(MINBOUND) {
						minval, _ := io.GetValueAsFloat64(sp.min)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
								if val < float32(minval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val <= float32(minval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
					if sp.ContentsEnum.IsSet(MAXBOUND) {
						maxval, _ := io.GetValueAsFloat64(sp.max)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
								if val > float32(maxval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val >= float32(maxval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
				case []float64:
					if sp.ContentsEnum.IsSet(EQUALITY) {
						eqval, _ := io.GetValueAsFloat64(sp.equal)
						for i, val := range col {
							if val != eqval {
								removalBitmap[i] = true // remove
							}
						}
					}
					if sp.ContentsEnum.IsSet(MINBOUND) {
						minval, _ := io.GetValueAsFloat64(sp.min)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
								if val < minval {
									removalBitmap[i] = true // remove
								}
							} else {
								if val <= minval {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
					if sp.ContentsEnum.IsSet(MAXBOUND) {
						maxval, _ := io.GetValueAsFloat64(sp.max)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
								if val > maxval {
									removalBitmap[i] = true // remove
								}
							} else {
								if val >= maxval {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
				case []int:
					if sp.ContentsEnum.IsSet(EQUALITY) {
						eqval, _ := io.GetValueAsInt64(sp.equal)
						for i, val := range col {
							if val != int(eqval) {
								removalBitmap[i] = true // remove
							}
						}
					}
					if sp.ContentsEnum.IsSet(MINBOUND) {
						minval, _ := io.GetValueAsInt64(sp.min)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
								if val < int(minval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val <= int(minval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
					if sp.ContentsEnum.IsSet(MAXBOUND) {
						maxval, _ := io.GetValueAsInt64(sp.max)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
								if val > int(maxval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val >= int(maxval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
				case []int32:
					if sp.ContentsEnum.IsSet(EQUALITY) {
						eqval, _ := io.GetValueAsInt64(sp.equal)
						for i, val := range col {
							if val != int32(eqval) {
								removalBitmap[i] = true // remove
							}
						}
					}
					if sp.ContentsEnum.IsSet(MINBOUND) {
						minval, _ := io.GetValueAsInt64(sp.min)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
								if val < int32(minval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val <= int32(minval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
					if sp.ContentsEnum.IsSet(MAXBOUND) {
						maxval, _ := io.GetValueAsInt64(sp.max)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
								if val > int32(maxval) {
									removalBitmap[i] = true // remove
								}
							} else {
								if val >= int32(maxval) {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
				case []int64:
					if sp.ContentsEnum.IsSet(EQUALITY) {
						eqval, _ := io.GetValueAsInt64(sp.equal)
						for i, val := range col {
							if val != eqval {
								removalBitmap[i] = true // remove
							}
						}
					}
					if sp.ContentsEnum.IsSet(MINBOUND) {
						minval, _ := io.GetValueAsInt64(sp.min)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
								if val < minval {
									removalBitmap[i] = true // remove
								}
							} else {
								if val <= minval {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
					if sp.ContentsEnum.IsSet(MAXBOUND) {
						maxval, _ := io.GetValueAsInt64(sp.max)
						for i, val := range col {
							if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
								if val > maxval {
									removalBitmap[i] = true // remove
								}
							} else {
								if val >= maxval {
									removalBitmap[i] = true // remove
								}
							}
						}
					}
				}
			}
		}
		outputColumnSeries.RestrictViaBitmap(removalBitmap)
	}

	/*
		Handle functions in Select List
	*/
	var selectListOutput *io.ColumnSeries
	var skipProjection bool // TODO: Only skip for SRF
	if !sr.IsSelectAll {
		for _, sl := range sr.SelectList {
			if sl.IsFunctionCall {
				if selectListOutput == nil {
					selectListOutput = io.NewColumnSeries()
				}
				// TODO: This only handles SRF
				skipProjection = true
				aggName := sl.FunctionCall.Name
				agg := AggRegistry[strings.ToLower(aggName)]
				if agg == nil {
					return nil, fmt.Errorf("No function in the UDA Registry named \"%s\"", aggName)
				}
				aggfunc, argMap := agg.New(sr.DisableVariableCompression)

				if sl.FunctionCall.IsAsterisk {
					/*
						If an asterisk is provided, use Epoch as the mapped input column
					*/
					argMap.MapRequiredColumn("*", io.DataShape{
						Name: "Epoch", Type: io.INT64,
					})
				} else {
					idList := sl.FunctionCall.GetIDs()
					err = argMap.PrepareArguments(idList)
					if err != nil {
						return nil, fmt.Errorf("Argument mapping error for %s: %s", aggName, err.Error())
					}
				}

				/*
					Initialize the Aggregate
						An agg may have init parameters, which are used only to initialize it
						These are single value literals (like '1Min')
				*/
				requiredInitDSV := aggfunc.GetInitArgs()
				requiredInitNames := io.GetNamesFromDSV(requiredInitDSV)

				initList := sl.FunctionCall.GetLiterals()
				if len(requiredInitNames) > len(initList) {
					return nil, fmt.Errorf(
						"Not enough init arguments for %s, need %d have %d",
						aggName,
						len(requiredInitNames),
						len(initList),
					)
				}
				// TODO: Handle different argument types from string
				var initArgList []string
				for _, lit := range initList {
					value := lit.Value.(string)
					value = value[1 : len(value)-1] // Strip the quotes
					initArgList = append(
						initArgList,
						value,
					)
				}
				aggfunc.Init(initArgList)

				/*
					Execute the aggregate function
				*/
				err := aggfunc.Accum(outputColumnSeries, sr.CatalogDirectory)
				if err != nil {
					return nil, err
				}
				functionResult := aggfunc.Output()
				if functionResult == nil {
					return nil, fmt.Errorf(
						"No result from aggregate %s",
						aggName)
				}

				for _, name := range functionResult.GetColumnNames() {
					outname := name
					if name != "Epoch" {
						if sl.IsAliased {
							outname = sl.Alias
						}
					}
					selectListOutput.AddColumn(
						outname,
						functionResult.GetColumn(name))
				}
			}
		}
		if selectListOutput != nil { // We had function calls in the select list, replace the output
			outputColumnSeries = io.NewColumnSeries()

			// Add an Epoch column if one doesn't exist
			ep := selectListOutput.GetByName("Epoch")
			if ep == nil {
				tNow := time.Now().UTC().Unix()
				var epochCol []int64
				for i := 0; i < selectListOutput.Len(); i++ {
					epochCol = append(epochCol, tNow)
				}
				outputColumnSeries.AddColumn("Epoch", epochCol)
			}
			for _, name := range selectListOutput.GetColumnNames() {
				outputColumnSeries.AddColumn(name,
					selectListOutput.GetColumn(name))
			}
		}
	}

	/*
		Handle column projection and aliases
	*/
	if !sr.IsSelectAll && !skipProjection {
		// Column projection
		err = outputColumnSeries.Project(keepList)
		if err != nil {
			return nil, err
		}
		// Column alias remapping on exit
		for _, item := range sr.SelectList {
			if item.IsAliased {
				err := outputColumnSeries.Rename(item.Alias, item.PrimaryName)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	/*
		Enforce LIMIT on the final results
	*/
	if sr.Limit != 0 {
		outputColumnSeries.RestrictLength(sr.Limit, io.FIRST)
	}

	return outputColumnSeries, nil
}

func (sr *SelectRelation) Explain() string {
	if sr != nil {
		jsonStruct, _ := json.Marshal(*sr)
		return string(jsonStruct)
	} else {
		return "{}"
	}
}

func (sr *SelectRelation) GetLeft() IMSTree {
	if sr.GetChildCount() == 0 {
		return nil
	} else {
		return sr.GetChild(0)
	}
}

func (sr *SelectRelation) GetRight() IMSTree {
	if sr.GetChildCount() < 2 {
		return nil
	} else {
		return sr.GetChild(1)
	}
}

/*
Utility Structures
*/

type StaticPredicateGroup map[string]*StaticPredicate

func NewStaticPredicateGroup() (spg StaticPredicateGroup) {
	return StaticPredicateGroup(make(map[string]*StaticPredicate))
}

func (spg StaticPredicateGroup) Add(column *ColumnReference) *StaticPredicate {
	name := column.GetName()
	if _, ok := spg[name]; !ok {
		spg[name] = NewStaticPredicate(column)
	}
	return spg[name]
}
func (spg StaticPredicateGroup) AddComparison(column *ColumnReference,
	op io.ComparisonOperatorEnum, value interface{}) {
	sp := spg.Add(column) // Will add if not already present
	sp.AddComparison(op, value)
}

func (spg StaticPredicateGroup) Merge(sp *StaticPredicate, IsOr bool) error {
	/*
		If IsOr is set, merge this predicate as an OR with the existing
		TODO: Implement OR predicate logic in the Static Predicate
	*/
	if sp == nil {
		return fmt.Errorf("Nil static predicate argumen")
	}
	tgtSP := spg.Add(sp.Column) // Adds a new SP if not already there
	if sp.ContentsEnum.IsSet(MINBOUND) {
		tgtSP.ContentsEnum.AddOption(MINBOUND)
		if sp.ContentsEnum.IsSet(INCLUSIVEMIN) {
			tgtSP.ContentsEnum.AddOption(INCLUSIVEMIN)
			tgtSP.AddComparison(io.GTE, sp.min)
		} else {
			tgtSP.AddComparison(io.GT, sp.min)
		}
	}
	if sp.ContentsEnum.IsSet(MAXBOUND) {
		tgtSP.ContentsEnum.AddOption(MAXBOUND)
		if sp.ContentsEnum.IsSet(INCLUSIVEMAX) {
			tgtSP.ContentsEnum.AddOption(INCLUSIVEMAX)
			tgtSP.AddComparison(io.LTE, sp.max)
		} else {
			tgtSP.AddComparison(io.LT, sp.max)
		}
	}
	if sp.ContentsEnum.IsSet(EQUALITY) {
		tgtSP.ContentsEnum.AddOption(EQUALITY)
		tgtSP.AddComparison(io.EQ, sp.equal)
	}
	if sp.ContentsEnum.IsSet(LIKEPATTERN) {
		tgtSP.ContentsEnum.AddOption(LIKEPATTERN)
		tgtSP.SetLike(sp.likePattern, sp.likeEsc)
	}
	if sp.ContentsEnum.IsSet(INLIST) {
		tgtSP.ContentsEnum.AddOption(INLIST)
		for _, item := range sp.inlist {
			tgtSP.inlist = append(tgtSP.inlist, item)
		}
	}
	return nil
}

type StaticPredicate struct {
	/*
		This stores the right hand side of an evaluation such as:

	*/
	Column               *ColumnReference
	inlist               []interface{}
	min, max, equal      interface{} // Comparison bounds
	likePattern, likeEsc string
	ContentsEnum         StaticPredicateContentsEnum
}

func NewStaticPredicate(column *ColumnReference) (sp *StaticPredicate) {
	sp = new(StaticPredicate)
	sp.Column = column
	return sp
}

func (sp *StaticPredicate) IsFalse() bool {
	/*
		If this predicate is provably false internally, return true
	*/
	match, _ := io.GenericComparison(sp.min, sp.max, io.GT)
	return match
}

func (sp *StaticPredicate) SetMin(newMin interface{}, inclusive bool) {
	sp.min = newMin
	sp.ContentsEnum.AddOption(MINBOUND)
	if inclusive {
		sp.ContentsEnum.AddOption(INCLUSIVEMIN)
	}

}
func (sp *StaticPredicate) SetMax(newMax interface{}, inclusive bool) {
	sp.max = newMax
	sp.ContentsEnum.AddOption(MAXBOUND)
	if inclusive {
		sp.ContentsEnum.AddOption(INCLUSIVEMAX)
	}
}
func (sp *StaticPredicate) SetEqual(newEQ interface{}) {
	sp.equal = newEQ
	sp.ContentsEnum.AddOption(EQUALITY)
}
func (sp *StaticPredicate) SetLike(pattern, esc string) {
	sp.likePattern = pattern
	sp.likeEsc = esc
	sp.ContentsEnum.AddOption(LIKEPATTERN)
}
func (sp *StaticPredicate) SetInlist(inlist []interface{}) {
	sp.inlist = inlist
	sp.ContentsEnum.AddOption(INLIST)
}

type StaticPredicateContentsEnum uint16

const (
	_ StaticPredicateContentsEnum = 1 << iota
	INLIST
	MINBOUND
	INCLUSIVEMIN // Boundary should include the min value
	MAXBOUND
	INCLUSIVEMAX // Boundary should include the max value
	EQUALITY
	LIKEPATTERN
)

func (cat *StaticPredicateContentsEnum) AddOption(option StaticPredicateContentsEnum) {
	*cat |= option
}
func (cat *StaticPredicateContentsEnum) DelOption(option StaticPredicateContentsEnum) {
	*cat &= ^option
}
func (cat *StaticPredicateContentsEnum) IsSet(checkOption ...StaticPredicateContentsEnum) bool {
	/*
		Returns true if all supplied options are set
	*/
	for _, co := range checkOption {
		if (*cat)&co != co {
			return false
		}
	}
	return true
}
func (cat *StaticPredicateContentsEnum) AnySet(checkOption ...StaticPredicateContentsEnum) bool {
	/*
		Returns true if any of the supplied options are set
	*/
	for _, co := range checkOption {
		if (*cat)&co == co {
			return true
		}
	}
	return false
}

func (sp *StaticPredicate) AddComparison(op io.ComparisonOperatorEnum,
	value interface{}) error {
	/*
		Set value of min/max/equal based on the operator
	*/
	switch op {
	case io.EQ:
		sp.equal = value
		sp.ContentsEnum.AddOption(EQUALITY)
	case io.LT, io.LTE:
		if sp.max == nil {
			sp.SetMax(value, op == io.LTE)
		} else {
			isWithin, err := io.GenericComparison(value, sp.max, op)
			if err != nil {
				return err
			}
			if !isWithin {
				sp.SetMax(value, op == io.LTE)
			}
		}
	case io.GT, io.GTE:
		if sp.min == nil {
			sp.SetMin(value, op == io.GTE)
		} else {
			isWithin, err := io.GenericComparison(value, sp.min, op)
			if err != nil {
				return err
			}
			if !isWithin {
				sp.SetMin(value, op == io.GTE)
			}
		}
	}
	return nil
}

type AliasedIdentifier struct {
	IsPrimary, IsAliased, IsFunctionCall bool
	PrimaryName, Alias                   string
	RuntimeExpression                    *ExpressionParse
	FunctionCall                         *FunctionCallReference
}

func NewAliasedIdentifier(name ...string) (ai *AliasedIdentifier) {
	ai = new(AliasedIdentifier)
	if len(name) != 0 {
		ai.IsPrimary = true
		ai.PrimaryName = name[0]
	}
	return ai
}

func (ai *AliasedIdentifier) AddRuntimeExpression(ep *ExpressionParse) {
	ai.RuntimeExpression = ep
}
func (ai *AliasedIdentifier) AddFunctionCall(fc *FunctionCallReference) {
	ai.FunctionCall = fc
	ai.IsFunctionCall = true
}
func (ai *AliasedIdentifier) AddAlias(alias string) {
	ai.IsAliased = true
	ai.Alias = alias
}
func (ai *AliasedIdentifier) String() (out string) {
	var buffer bytes.Buffer
	buffer.WriteString("Identifier: ")
	if ai.IsPrimary {
		buffer.WriteString(fmt.Sprintf("Primary Name: %s ", ai.PrimaryName))
	} else {
		buffer.WriteString("Runtime Expression: ")
	}
	if ai.IsAliased {
		buffer.WriteString(fmt.Sprintf("Alias: %s ", ai.Alias))
	}
	return buffer.String()
}

func SourceValidator(sourceDSV []io.DataShape, selectList []*AliasedIdentifier) (validates bool,
	missing, keepList, projectionList []string, err error) {
	if selectList == nil {
		return true, nil, nil, nil, nil
	}
	/*
		Given a source's DataShapes, verify that the target ID list is found within it
	*/
	// Get target names from identifiers
	for _, id := range selectList {
		switch {
		case id.IsFunctionCall:
			if id.FunctionCall.IsAsterisk {
				keepList = append(keepList, "Epoch")
			} else {
				/*
					Preprocess the parameters for parameterName::COLUMN_NAME pairs
				*/
				for _, token := range id.FunctionCall.GetIDs() {
					args := strings.Split(token, "::")
					keepList = append(keepList, args[len(args)-1])
				}
			}
		case id.IsPrimary:
			keepList = append(keepList, id.PrimaryName)
		}
	}
	sourceNames := io.GetNamesFromDSV(sourceDSV)
	targetNamesSet, err := io.NewAnySet(keepList)
	if err != nil {
		return false, nil, nil, nil, fmt.Errorf("Unable to build set for target")
	}
	i_missingIDs := targetNamesSet.Subtract(sourceNames)
	var missingIDs []string
	if i_missingIDs != nil {
		missingIDs = i_missingIDs.([]string)
	}
	if len(missingIDs) != 0 {
		return false, missingIDs, nil, nil, nil
	}
	/*
		Find the list of names in the source not needed by the target
	*/
	sourceNamesSet, _ := io.NewAnySet(sourceNames)
	projectionList = sourceNamesSet.Subtract(keepList).([]string)

	return true, nil, keepList, projectionList, nil
}

/*
Utility functions
*/
