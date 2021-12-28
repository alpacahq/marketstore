package sqlparser

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type InsertIntoStatement struct {
	ExecutableStatement
	SelectRelation *SelectRelation
	QueryText      string
	TableName      string
	// ColumnAliases are the names of the columns to insert data.
	// e.g. INSERT INTO foo (a, b, c) SELECT * FROM bar; -> ["a", "b", "c"] are the column aliases.
	ColumnAliases []string
}

func NewInsertIntoStatement(tableName, queryText string, selectRelation *SelectRelation) (is *InsertIntoStatement) {
	is = new(InsertIntoStatement)
	is.QueryText = queryText
	is.TableName = tableName
	is.SelectRelation = selectRelation
	return is
}

func (is *InsertIntoStatement) Materialize(aggRunner *AggRunner, catDir *catalog.Directory) (outputColumnSeries *io.ColumnSeries, err error) {
	// Call Materialize on any child relations.
	// inputColumnSeries includes Epoch column
	inputColumnSeries, err := is.SelectRelation.Materialize(aggRunner, catDir)
	if err != nil {
		return nil, err
	}

	// Check the input, report contents
	if inputColumnSeries != nil {
		if inputColumnSeries.Len() != 0 {
			fmt.Printf("Query returned %d rows, inserting into: %s\n",
				inputColumnSeries.Len(), is.TableName)
		} else {
			return nil, nil
		}
	}

	/*
		Map the target table's columns to the results
	*/
	targetMK := io.NewTimeBucketKey(is.TableName)
	if targetMK == nil {
		return nil, fmt.Errorf("table name must be in the format `one/two/three`, have: %s",
			is.TableName)
	}

	fi, err := catDir.GetLatestTimeBucketInfoFromKey(targetMK)
	if err != nil {
		return nil, err
	}
	targetDSV := fi.GetDataShapesWithEpoch()

	/*
		Use column aliases to select required target columns in mapping
	*/
	var targetColumnNames []string // Final result column names will be here
	if is.ColumnAliases != nil {
		targetColumnNames = is.ColumnAliases
	} else {
		for _, shape := range targetDSV {
			targetColumnNames = append(
				targetColumnNames,
				shape.Name,
			)
		}
	}

	/*
		Target Column names now has the required columns in it
		We now need to find those columns in the results
	*/
	inputColumnNames := io.GetNamesFromDSV(inputColumnSeries.GetDataShapes())
	as, _ := io.NewAnySet(inputColumnNames)
	if !as.Contains(targetColumnNames) {
		// Calculate the remainder of names not present
		targetSet, _ := io.NewAnySet(targetColumnNames)
		residue := targetSet.Subtract(inputColumnNames)
		return nil, fmt.Errorf(
			"\nUnable to find these columns: %v needed for INSERT into target table %s\nTry %s",
			residue,
			is.TableName,
			"using column aliases to exclude the needed columns from the select list.\n"+
				"Example: if foo is foo(a,b,c,d) and bar is a,b,c:\n"+
				"\tINSERT INTO foo (a, b, c) SELECT * FROM bar;",
		)
	}

	// Get the time with nanoseconds included if available, prior to projection
	// indexTime, err := inputColumnSeries.GetTime()

	// Columns are matched - Now project out all but the target column names
	inputColumnSeries.Project(targetColumnNames)

	/*
		Write the data
	*/
	isVariableLength := inputColumnSeries.GetColumn("Nanoseconds") != nil

	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*targetMK, inputColumnSeries)
	if err = executor.WriteCSM(csm, isVariableLength); err != nil {
		return nil, err
	}
	// --------

	outputColumnSeries = io.NewColumnSeries()
	outputColumnSeries.AddColumn("Epoch",
		[]int64{time.Now().UTC().Unix()})
	outputColumnSeries.AddColumn("Rows Written",
		[]float32{float32(inputColumnSeries.Len())})

	return outputColumnSeries, nil
}

func (is *InsertIntoStatement) Explain() string {
	if is != nil {
		jsonStruct, _ := json.Marshal(*is)
		return string(jsonStruct)
	} else {
		return "{}"
	}
}

func (is *InsertIntoStatement) GetLeft() IMSTree {
	if is.GetChildCount() == 0 {
		return nil
	} else {
		return is.GetChild(0)
	}
}

func (is *InsertIntoStatement) GetRight() IMSTree {
	if is.GetChildCount() < 2 {
		return nil
	} else {
		return is.GetChild(1)
	}
}

/*
Utility Structures
*/
