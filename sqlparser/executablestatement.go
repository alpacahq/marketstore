package sqlparser

import (
	"bytes"
	"fmt"
	"github.com/alpacahq/marketstore/v4/catalog"
	"reflect"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type ExecutableStatement struct {
	QueryTree
	BaseSQLQueryTreeVisitor
	nodeCursor                 *ExecutableStatement
	pendingSP                  *StaticPredicate
	IsExplain                  bool
	DisableVariableCompression bool
	CatalogDirectory           *catalog.Directory
}

func NewExecutableStatement(disableVariableCompression bool, catDir *catalog.Directory, qtree ...IMSTree,
) (es *ExecutableStatement, err error) {
	es = &ExecutableStatement{
		DisableVariableCompression: disableVariableCompression,
		CatalogDirectory:           catDir,
	}
	es.nodeCursor = es
	if len(qtree) > 0 {
		i_err := es.Visit(qtree[0])
		if err, ok := i_err.(error); ok {
			return nil, err
		}
	}
	return es, nil
}

func (es *ExecutableStatement) GetPendingStaticPredicateGroup() (spg StaticPredicateGroup, err error) {
	if sr, ok := es.nodeCursor.payload.(*SelectRelation); !ok {
		return nil, fmt.Errorf("No Select Relation in progress")
	} else {
		return sr.StaticPredicates, nil
	}
}

func (es *ExecutableStatement) Materialize() (cs *io.ColumnSeries, err error) {
	var child_cs *io.ColumnSeries
	if es.GetChildCount() != 0 {
		node := es.GetChild(0)
		switch ctx := node.(type) {
		case *ExecutableStatement:
			//fmt.Println("Materialize Executable Statement")
			child_cs, err = ctx.Materialize()
		case *SelectRelation:
			//fmt.Println("Materialize Select Relation")
			child_cs, err = ctx.Materialize()
		case *ExplainStatement:
			//fmt.Println("Materialize Explain Statement")
			child_cs, err = ctx.Materialize()
		case *InsertIntoStatement:
			//fmt.Println("Materialize InsertInto Statement")
			child_cs, err = ctx.Materialize()
		}
		if err != nil {
			return nil, err
		}
		return child_cs, nil
	} else {
		switch ctx := es.nodeCursor.payload.(type) {
		case *SelectRelation:
			//			fmt.Println("Materialize Select Relation Statement (no children)")
			cs, err = ctx.Materialize()
			return cs, err
		default:
			//			fmt.Println("Materialize Default (nil)")
			return nil, nil
		}
	}
}

func (es *ExecutableStatement) Visit(tree IMSTree) interface{} {
	return tree.Accept(es)
}
func (es *ExecutableStatement) VisitChildren(tree IMSTree) interface{} {
	for _, child := range tree.GetChildren() {
		retval := child.Accept(es)
		if retval != nil {
			return retval
		}
	}
	return nil
}

func (es *ExecutableStatement) VisitStatementsParse(ctx *StatementsParse) interface{} {
	child := ctx.GetChild(0)
	return es.Visit(child)
}
func (es *ExecutableStatement) VisitStatementParse(ctx *StatementParse) interface{} {
	switch ctx.statementType {
	case QUERY_STMT:
		retval := QueryWalk(es.nodeCursor, ctx.query)
		if err, ok := retval.(error); ok {
			return err
		}
	case EXPLAIN_STMT:
		context := ctx.statement.(*StatementParse)
		es.AddChild(NewExplainStatement(context, ctx.QueryText))
	case INSERT_INTO_STMT:
		var err error
		es.nodeCursor, err = NewExecutableStatement(es.DisableVariableCompression, es.CatalogDirectory, ctx.query)
		if err != nil {
			return fmt.Errorf("Unable to create executable query")
		}
		retval := QueryWalk(es.nodeCursor, ctx.query)
		if err, ok := retval.(error); ok {
			return err
		}
		sr := es.nodeCursor.payload.(*SelectRelation)
		es.nodeCursor = es

		// Get Table Name
		i_tableName := es.nodeCursor.Visit(ctx.tableName)
		// Get Column Aliases
		var columnAliases []string
		if ctx.columnAliases != nil {
			for _, child := range ctx.columnAliases.GetChildren() {
				cctx := child.(*IDParse)
				columnAliases = append(columnAliases, cctx.name)
			}
		}
		is := NewInsertIntoStatement(i_tableName.(string), ctx.QueryText, sr,
			es.DisableVariableCompression, es.CatalogDirectory,
		)
		is.TableName = i_tableName.(string)
		is.ColumnAliases = columnAliases

		es.AddChild(is)
	default:
		return fmt.Errorf("Unsupported statement type: %s", ctx.statementType.String())
	}
	return nil
}

func QueryWalk(es *ExecutableStatement, i_ctx IMSTree) interface{} {
	var ctx *QueryParse
	var ok bool
	if ctx, ok = i_ctx.(*QueryParse); !ok {
		return fmt.Errorf("Unable to get *QueryParse")
	}
	var retval interface{}
	retval = es.nodeCursor.Visit(ctx)
	if retval != nil {
		for {
			switch value := retval.(type) {
			case error:
				return value
			case *QueryNoWithParse:
				retval = es.nodeCursor.Visit(value)
			case *QueryTermParse:
				retval = es.nodeCursor.Visit(value)
			case *QueryPrimaryParse:
				retval = es.nodeCursor.Visit(value)
			case *QuerySpecificationParse:
				retval = es.nodeCursor.Visit(value)
				if err, ok := retval.(error); ok {
					return err
				}
			default:
				return nil
			}
		}
	}
	return nil
}

func (es *ExecutableStatement) VisitQueryParse(ctx *QueryParse) interface{} {
	return ctx.queryNoWith
}
func (es *ExecutableStatement) VisitQueryNoWithParse(ctx *QueryNoWithParse) interface{} {
	if ctx.sortItems != nil {
		// TODO: Support ORDER BY
		return fmt.Errorf("Unsupported statement type: %s", "Query with ORDER BY")
	}

	sr := NewSelectRelation(es.DisableVariableCompression, es.CatalogDirectory)
	sr.Limit = ctx.limit

	es.nodeCursor.payload = sr // For retrieval of the dynamic type later
	return ctx.queryTerm
}
func (es *ExecutableStatement) VisitQueryTermParse(ctx *QueryTermParse) interface{} {
	if ctx.queryPrimary == nil {
		// TODO: Support JOIN
		return fmt.Errorf("Unsupported statement type: %s", "Join")
	}
	return ctx.queryPrimary
}
func (es *ExecutableStatement) VisitQueryPrimaryParse(ctx *QueryPrimaryParse) interface{} {
	if ctx.querySpec == nil {
		// TODO: Support TABLE, INLINE TABLE and SUBQUERY
		return fmt.Errorf("Unsupported statement type: %s", "TABLE, INLINE TABLE or SUBQUERY")
	}
	sr := es.nodeCursor.payload.(*SelectRelation)
	sr.IsPrimary = true
	if ctx.subquery != nil {
		//fmt.Println("Visit Query Primary subquery")
		sr.IsPrimary = false
		node, err := NewExecutableStatement(es.DisableVariableCompression, es.CatalogDirectory, ctx.subquery)
		if err != nil {
			return err
		}
		es.AddChild(node)
	}
	return ctx.querySpec
}
func (es *ExecutableStatement) VisitQuerySpecificationParse(ctx *QuerySpecificationParse) interface{} {
	/*
		This is a terminal node for the current executable statement
		Only SELECT is possible in this node

		      SELECT setQuantifier? selectItem (',' selectItem)*
		      (FROM relation (',' relation)*)?
		      (WHERE where=booleanExpression)?
		      (GROUP BY groupBy)?
		      (HAVING having=booleanExpression)?

	*/
	sr := es.nodeCursor.payload.(*SelectRelation)
	sr.StaticPredicates = NewStaticPredicateGroup()
	sr.SetQuantifier = ctx.setQuantifier

	/*
		Gather Select list
	*/
	for _, item := range ctx.selectItems {
		cctx := item.(*SelectItemParse)
		if cctx.IsSelectAll {
			sr.IsSelectAll = true
			break
		}
		var aliasName string
		if cctx.alias != nil {
			aliasName = es.nodeCursor.Visit(cctx.alias).(string)
		}
		icr := es.nodeCursor.Visit(cctx.expression)
		switch cr := icr.(type) {
		// TODO: Function Call goes here
		case *ColumnReference:
			if cctx.alias != nil {
				iname := es.nodeCursor.Visit(cctx.alias) // Identifier
				switch name := iname.(type) {
				case string:
					cr.Value.AddAlias(name)
				}
			}
			sr.SelectList = append(sr.SelectList, cr.Value)
		case *FunctionCallReference:
			ai := NewAliasedIdentifier()
			ai.AddFunctionCall(cr)
			if len(aliasName) != 0 {
				ai.Alias = aliasName
				ai.IsAliased = true
			}
			sr.SelectList = append(sr.SelectList, ai)
		}
	}
	if sr.IsSelectAll && len(ctx.selectItems) > 1 {
		return fmt.Errorf("Unsupported option: Multiple select items specified along with an asterisk for all")
	}

	/*
		Gather table references
	*/
	for _, item := range ctx.relations {
		i_tableName := es.nodeCursor.Visit(item)
		//fmt.Println("Gathering relations: ", i_tableName, item, reflect.ValueOf(item).Type())
		switch value := i_tableName.(type) {
		case string:
			sr.PrimaryTargetName = append(sr.PrimaryTargetName, value)
		case *SelectRelation:
			//fmt.Println("Gathered subquery")
			sr.IsPrimary = false
			sr.Subquery = value
		case error:
			return value
		}
	}

	/*
		Retrieve conforming Epoch column predicates from the WHERE
		expressions so they can be pushed down.

		Conforming predicates are of the form:
		       Epoch [<,>,==,>=,<=] time_specification
		   or:
		       Epoch BETWEEN time_specification AND time_specification

	*/
	if ctx.where != nil {
		i_err := es.nodeCursor.Visit(ctx.where) // BooleanExpression
		if err, ok := i_err.(error); ok {
			return err
		}
	}
	return nil
}
func (es *ExecutableStatement) VisitExpressionParse(ctx *ExpressionParse) interface{} {
	/*
		1 Child, one of ValueExpression or BooleanExpression
	*/
	return es.nodeCursor.Visit(ctx.GetChild(0))
}
func (es *ExecutableStatement) VisitValueExpressionParse(ctx *ValueExpressionParse) interface{} {
	/*
		1 Child, 5 options
	*/
	child := ctx.GetChild(0)
	switch cctx := child.(type) {
	case *PrimaryExpressionParse: // Primary Expression
		return es.nodeCursor.Visit(cctx)
	default:
		// TODO: Support non primary expressions
		return fmt.Errorf("Only Primary Expressions supported")
	}
}
func (es *ExecutableStatement) VisitPrimaryExpressionParse(ctx *PrimaryExpressionParse) interface{} {
	/*
		1 Child, lots of options
	*/
	switch ctx.primaryType {
	case NULL_LITERAL, STRING_LITERAL, BINARY_LITERAL, DECIMAL_LITERAL, INTEGER_LITERAL, BOOLEAN_LITERAL:
		return NewLiteral(ctx.payload, ctx.primaryType)
	case COLUMN_REFERENCE:
		retval := es.nodeCursor.Visit(ctx.GetChild(0))
		switch value := retval.(type) {
		case string:
			cr := NewColumnReference(value)
			return cr
		default:
			return fmt.Errorf("Non string returned as column reference")
		}
	case FUNCTION_CALL:
		retval := es.nodeCursor.Visit(ctx.GetChild(0))
		switch value := retval.(type) {
		case *FunctionCallReference:
			return value
		default:
			return fmt.Errorf("Unexpected non FunctionCall returned")
		}
	case PARENTHESIZED_EXPRESSION:
		return es.nodeCursor.Visit(ctx.GetChild(0))
	default:
		// TODO: Support other than column refs
		return fmt.Errorf("Unsupported primary expression found: %s",
			ctx.primaryType.String())
	}
}
func (es *ExecutableStatement) VisitIDParse(ctx *IDParse) interface{} {
	return ctx.name
}
func (es *ExecutableStatement) VisitRelationParse(ctx *RelationParse) interface{} {
	return es.nodeCursor.Visit(ctx.sampled)
}
func (es *ExecutableStatement) VisitSampledRelationParse(ctx *SampledRelationParse) interface{} {
	return es.nodeCursor.Visit(ctx.aliasedRelation)
}
func (es *ExecutableStatement) VisitAliasedRelationParse(ctx *AliasedRelationParse) interface{} {
	if ctx.hasAliases {
		// TODO: Support table aliases
		return fmt.Errorf("Table Aliases not supported")
	}
	return es.nodeCursor.Visit(ctx.relationPrimary)
}
func (es *ExecutableStatement) VisitRelationPrimaryParse(ctx *RelationPrimaryParse) interface{} {
	switch {
	case ctx.IsTableName || ctx.IsRelation:
		return es.nodeCursor.Visit(ctx.GetChild(0))
	case ctx.IsSubquery:
		//fmt.Println("Visit IsSubquery")
		/*
			childNode, _ := NewExecutableStatement()
			es.AddChild(childNode)
			retval := QueryWalk(childNode, ctx.GetChild(0).(*QueryParse))
			if err, ok := retval.(error); ok {
				return err
			}
			fmt.Println("Retval: ", retval)
			if sr, ok := childNode.payload.(*SelectRelation); ok {
				return sr
			} else {
				return fmt.Errorf("Unable to load subquery")
			}
		*/
		if sr, ok := es.payload.(*SelectRelation); ok {
			newNode, _ := NewExecutableStatement(es.DisableVariableCompression, es.CatalogDirectory)
			retval := QueryWalk(newNode, ctx.GetChild(0).(*QueryParse))
			if err, ok := retval.(error); ok {
				return err
			}
			// Get the subquery relation
			if srSub, ok := newNode.payload.(*SelectRelation); ok {
				sr.Subquery = srSub
				return srSub
			}
		}
	default:
		return fmt.Errorf("Unsupported Primary Relation type")
	}
	return nil
}
func (es *ExecutableStatement) VisitQualifiedNameParse(ctx *QualifiedNameParse) interface{} {
	var buffer bytes.Buffer
	numberElements := ctx.GetChildCount()
	for i, child := range ctx.GetChildren() {
		id := es.nodeCursor.Visit(child)
		name := id.(string)
		buffer.WriteString(name)
		if i < numberElements-1 {
			buffer.WriteString("/")
		}
	}
	return buffer.String()
}

func (es *ExecutableStatement) VisitBooleanExpressionParse(ctx *BooleanExpressionParse) interface{} {
	/*
		Search for static predicates and return them
		Both left and right types are ValueExpression
	*/
	spg, err := es.GetPendingStaticPredicateGroup()
	if err != nil {
		return err
	}

	var done bool
	doneRight := (ctx.right == nil) // No right side

	node := ctx.left
	for {
		if done {
			if !doneRight {
				node = ctx.right
				done = false
				doneRight = true
			} else {
				break
			}
		}
		i_value := es.nodeCursor.Visit(node) // Descend left
		if i_value == nil {
			done = true
		} else {
			switch value := i_value.(type) {
			case *ColumnReference:
				// Create new predicate for this column
				es.nodeCursor.pendingSP = NewStaticPredicate(value)
				// Descend to merge static predicates into this column
				i_value = es.nodeCursor.Visit(ctx.predicate)
				if err, ok := i_value.(error); ok {
					return err
				}
				err := spg.Merge(es.nodeCursor.pendingSP, false) // AND predicate
				if err != nil {
					return err
				}
				es.nodeCursor.pendingSP = nil
				done = true
			case *ExpressionParse:
				node = value // Continue to descend left
			case *ValueExpressionParse:
				node = value // Continue to descend left
			case *BooleanExpressionParse:
				node = value // Continue to descend left
			case error:
				return value
			case nil:
				done = true
			default:
				return fmt.Errorf("Unknown type: %s", reflect.ValueOf(value).Type())
			}

		}
	}
	return nil
}
func (es *ExecutableStatement) VisitPredicateParse(ctx *PredicateParse) interface{} {
	node := ctx.GetChild(0)
	switch node.(type) {
	case *ComparisonParse, *BetweenParse:
		return es.nodeCursor.Visit(node)
	case *QuantifiedComparisonParse:
		return fmt.Errorf("Quantified Comparisons (ALL/ANY/SOME) not supported")
	case *InListParse, *InSubqueryParse, *LikeParse, *NullPredicateParse, *DistinctFromParse:
		// TODO: Implement dynamic predicates (and inlist)
		return fmt.Errorf("Unsupported predicate type, only static types are supported")
	}
	return nil
}

func (es *ExecutableStatement) VisitBetweenParse(ctx *BetweenParse) interface{} {
	i_literal := es.nodeCursor.Visit(ctx.lower)
	if literal, ok := i_literal.(*Literal); !ok {
		return fmt.Errorf("Dynamic predicate bounds not supported")
	} else {
		/*
			Make sure that the literal represents a numeric quantity
		*/
		err := CoerceToNumeric(literal)
		if err != nil {
			return err
		}
		if ctx.IsNot {
			es.nodeCursor.pendingSP.AddComparison(io.LTE, literal.Value)
		} else {
			es.nodeCursor.pendingSP.AddComparison(io.GT, literal.Value)
		}
	}

	i_literal = es.nodeCursor.Visit(ctx.upper)
	if literal, ok := i_literal.(*Literal); !ok {
		return fmt.Errorf("Dynamic predicate bounds not supported")
	} else {
		/*
			Make sure that the literal represents a numeric quantity
		*/
		err := CoerceToNumeric(literal)
		if err != nil {
			return err
		}
		if ctx.IsNot {
			es.nodeCursor.pendingSP.AddComparison(io.GTE, literal.Value)
		} else {
			es.nodeCursor.pendingSP.AddComparison(io.LT, literal.Value)
		}
	}
	return nil
}

func (es *ExecutableStatement) VisitComparisonParse(ctx *ComparisonParse) interface{} {
	i_literal := es.nodeCursor.Visit(ctx.right)
	if literal, ok := i_literal.(*Literal); !ok {
		return fmt.Errorf("Dynamic predicate bounds not supported")
	} else {
		/*
			Make sure that the literal represents a numeric quantity
		*/
		err := CoerceToNumeric(literal)
		if err != nil {
			return err
		}
		es.nodeCursor.pendingSP.AddComparison(ctx.comparisonOperator, literal.Value)
	}
	return nil
}

func (es *ExecutableStatement) VisitFunctionCallParse(ctx *FunctionCallParse) interface{} {
	i_name := es.nodeCursor.Visit(ctx.qualifiedName)
	name, ok := i_name.(string)
	if !ok {
		return fmt.Errorf("Error parsing function name")
	}

	var args []interface{}
	if ctx.hasAsterisk {
		fc := NewFunctionCallReference(name, args)
		fc.IsAsterisk = true
		return fc
	}
	for _, expr := range ctx.expressionList {
		i_value := es.nodeCursor.Visit(expr)
		switch value := i_value.(type) {
		case *ColumnReference:
			args = append(args, value.Value)
		case *Literal:
			args = append(args, value)
		default:
			return fmt.Errorf("Error parsing column ref")
		}
	}
	return NewFunctionCallReference(name, args)
}

/*
Primary Expression Datatypes
*/
type FunctionCallReference struct {
	Name       string
	IsAsterisk bool
	Args       []interface{}
}

func NewFunctionCallReference(name string, args []interface{}) *FunctionCallReference {
	fc := new(FunctionCallReference)
	fc.Name = name
	fc.Args = args
	return fc
}

func (fc *FunctionCallReference) GetIDs() (idList []string) {
	for _, i_arg := range fc.Args {
		switch arg := i_arg.(type) {
		case *AliasedIdentifier:
			if arg.IsPrimary {
				idList = append(idList, arg.PrimaryName)
			}
		}
	}
	return idList
}

func (fc *FunctionCallReference) GetLiterals() (literals []*Literal) {
	for _, i_arg := range fc.Args {
		switch arg := i_arg.(type) {
		case *Literal:
			literals = append(literals, arg)
		}
	}
	return literals
}

type ColumnReference struct {
	Value *AliasedIdentifier
}

func NewColumnReference(name string) (cr *ColumnReference) {
	cr = new(ColumnReference)
	cr.Value = NewAliasedIdentifier(name)
	return cr
}
func (cr *ColumnReference) GetName() string {
	return cr.Value.PrimaryName
}
func (cr *ColumnReference) GetAlias() string {
	return cr.Value.Alias
}

type Literal struct {
	Value interface{}
	Type  PrimaryExpressionEnum
}

func NewLiteral(value interface{}, pType PrimaryExpressionEnum) (li *Literal) {
	li = new(Literal)
	li.Value = value
	li.Type = pType
	return li
}

/*
Utility Structs and Functions
*/
func CoerceToNumeric(literal *Literal) (err error) {
	switch literal.Type {
	case STRING_LITERAL:
		// We need to coerce the string into a numeric and that means time/date
		//	Mon Jan 2 15:04:05 -0700 MST 2006
		value := literal.Value.(string)
		// Strip off the single quotes
		value = value[1 : len(value)-1]
		formats := []string{
			"2006-01-02-15:04:05 MST",
			"2006-01-02-15:04:05",
			"2006-01-02-15:04",
			"2006-01-02",
		}

		var t time.Time
		for _, formatString := range formats {
			t, err = time.Parse(formatString, value)
			if err == nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("Unable to convert string to date: %s",
				value)
		}
		literal.Value = t.Unix()
		literal.Type = INTEGER_LITERAL
	case NULL_LITERAL:
		return fmt.Errorf("Unable to convert NULL to a numeric")
	}
	return nil
}
