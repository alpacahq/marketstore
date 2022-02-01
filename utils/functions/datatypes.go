package functions

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type ArgumentMap struct {
	/*
	  Maps between a required set of input column names and a
	  provided list of column names
	*/
	nameMap map[string][]io.DataShape // ex: NameMap["parameterName"] = ["col1", "col2"]
	/*
		Stored using the parameter name as the map key
	*/
	requiredMap, optionalMap map[string]io.DataShape

	aliases map[string]string

	requiredNames, optionalNames []string
}

func NewArgumentMap(requiredDSV []io.DataShape, optionalDSV ...io.DataShape) *ArgumentMap {
	/*
		InputColumns is a list of input column names - 1:N mapping to user columns
		Example: "CandlePrice" -> "Bid", "Ask"
	*/
	am := new(ArgumentMap)
	am.nameMap = make(map[string][]io.DataShape)
	am.requiredMap = make(map[string]io.DataShape)
	am.optionalMap = make(map[string]io.DataShape)
	for _, col := range requiredDSV {
		am.requiredMap[col.Name] = col
		am.requiredNames = append(am.requiredNames, col.Name)
	}
	for _, col := range optionalDSV {
		am.optionalMap[col.Name] = col
		am.optionalNames = append(am.optionalNames, col.Name)
	}
	/*
		Initialize aliases to match the inputs
	*/
	am.aliases = make(map[string]string)
	for _, name := range am.requiredNames {
		am.aliases[name] = name
	}
	for _, name := range am.optionalNames {
		am.aliases[name] = name
	}

	return am
}

func (am *ArgumentMap) GetAliasedColumnNames() (aliasNames []string) {
	for _, name := range am.requiredNames {
		aliasNames = append(aliasNames, am.aliases[name])
	}
	for _, name := range am.optionalNames {
		aliasNames = append(aliasNames, am.aliases[name])
	}
	return aliasNames
}

func (am *ArgumentMap) SetAlias(requiredName, aliasName string) {
	am.aliases[requiredName] = aliasName
}

func (am *ArgumentMap) GetMappedColumns(requiredName string) (userColumns []io.DataShape) {
	return am.nameMap[requiredName]
}

func (am *ArgumentMap) MapRequiredColumn(requiredName string, userColumns ...io.DataShape) {
	/*
		One native input column can map to multiple user inputs, which are combined to
		create the single native input. In some cases they might be summed, in others
		they might be averaged - it's up to the function implementation to choose.
	*/
	if _, found := am.nameMap[requiredName]; found {
		// Entry Exists: Need to merge this into existing mapping
		existingCol := am.nameMap[requiredName]
		existingCol = append(existingCol, userColumns...)
		am.nameMap[requiredName] = existingCol
	} else {
		am.nameMap[requiredName] = userColumns
	}
}

func (am *ArgumentMap) Validate() (unmapped []io.DataShape) {
	/*
		Must call MapRequiredColumn() for each required column.
	*/
	if len(am.nameMap) == 0 {
		return nil
	}
	for _, ds := range am.requiredMap {
		name := ds.Name
		if _, ok := am.nameMap[name]; !ok {
			unmapped = append(unmapped, ds)
		} else {
			/*
				Secondary validation - data type mismatch
			*/
			mapCols := am.nameMap[name]
			requiredCol := am.requiredMap[name]
			for _, col := range mapCols {
				if requiredCol.Type != col.Type {
					unmapped = append(unmapped, ds)
				}
			}
		}
	}
	return unmapped
}

func (am *ArgumentMap) String() (st string) {
	var buffer bytes.Buffer
	buffer.WriteString("Required columns: ")
	for _, ds := range am.requiredMap {
		buffer.WriteString(ds.String() + ",")
	}
	buffer.WriteString(" Optional columns: ")
	for _, ds := range am.optionalMap {
		buffer.WriteString(ds.String() + ",")
	}
	buffer.WriteString(" Aliases: ")
	for _, name := range am.GetAliasedColumnNames() {
		buffer.WriteString(name + ",")
	}
	return buffer.String()
}

func (am *ArgumentMap) PrepareArguments(inputs []string) (err error) {
	/*
		We allow a mixture of named and positional parameters.
		For example, if foo requires (A, B) and optional (C), these are all valid:
			Positional:
				foo(A, B) and foo(A, B, C)
			Named:
				foo(A=i, B=j, C=k) and foo(C=k, A=i, B=j)
			Mixed:
				foo(B=j, A) and foo(C=k, A, B) and foo(A, C=k, B=j)

		The order of parameter binding evaluation is:
			1) Named
			2) Positional required
			3) Positional optional
	*/
	/*
		Check for insufficient number of params to meet required
	*/
	if len(am.requiredNames) > len(inputs) {
		return fmt.Errorf("have %s, need %s", inputs, am.requiredNames)
	}

	/*
		Use named params first: look for "parameterName:COLUMN_NAME"
	*/
	var inputsRemaining []string
	for _, token := range inputs {
		args := strings.Split(token, "::")
		if len(args) == 2 { // We have a parameterName:COLUMN_NAME pair
			am.MapRequiredColumn(args[0], io.DataShape{
				Name: args[1], Type: io.FLOAT32,
			})
		} else { // Add for second stage processing
			inputsRemaining = append(inputsRemaining, token)
		}
	}

	/*
		Construct a array of not filled params - needs to be in order of required
	*/

	var unmappedReqs, unmappedOpts []string
	for _, name := range am.requiredNames {
		if _, ok := am.nameMap[name]; !ok {
			unmappedReqs = append(unmappedReqs, name)
		}
	}
	// fmt.Println("Unmapped Reqs:", unmappedReqs)
	for _, name := range am.optionalNames {
		if _, ok := am.nameMap[name]; !ok {
			unmappedOpts = append(unmappedOpts, name)
		}
	}
	// fmt.Println("Unmapped Opts:", unmappedOpts)

	/*
		Check to see if there are enough remaining input params to fulfill the reqs
	*/
	if len(unmappedReqs) > len(inputsRemaining) {
		return fmt.Errorf("insufficient args: have %s, required %s", inputs, am.requiredNames)
	}
	/*
		Second stage - positional filling for required params
	*/
	//fmt.Println("inputs:", inputsRemaining, "requiredNames:", am.requiredNames)
	//fmt.Println("nameMap:", am.nameMap)
	//fmt.Println("unmapped reqs:", unmappedReqs, "optional:", unmappedOpts)
	var i int
	for _, requiredName := range unmappedReqs {
		am.MapRequiredColumn(requiredName, io.DataShape{
			Name: inputsRemaining[i], Type: io.FLOAT32,
		})
		i++
	}
	/*
		Consume any remaining inputs as positional optional parameters
	*/
	numRemaining := len(inputsRemaining) - (i + 1)
	if numRemaining > 0 {
		for _, optionalName := range unmappedOpts {
			am.MapRequiredColumn(optionalName, io.DataShape{
				Name: inputsRemaining[i], Type: io.FLOAT32,
			})
			i++
		}
	}

	numRemaining = len(inputsRemaining) - i
	if numRemaining != 0 {
		return fmt.Errorf("extra args used: have %s, required %s, optional %s",
			inputs, am.requiredNames, am.optionalNames)
	}
	return nil
}

/*
Utility Functions
*/
