package sqlparser

import (
	"encoding/json"
	"fmt"
	"github.com/alpacahq/marketstore/v4/catalog"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type InsertIntoStatement struct {
	ExecutableStatement
	SelectRelation *SelectRelation
	QueryText      string
	TableName      string
	ColumnAliases  []string
}

func NewInsertIntoStatement(tableName, queryText string, selectRelation *SelectRelation,
	catDir *catalog.Directory) (is *InsertIntoStatement) {
	is = new(InsertIntoStatement)
	is.QueryText = queryText
	is.TableName = tableName
	is.SelectRelation = selectRelation
	is.CatalogDirectory = catDir
	return is
}

func (is *InsertIntoStatement) Materialize() (outputColumnSeries *io.ColumnSeries, err error) {
	// Call Materialize on any child relations
	inputColumnSeries, err := is.SelectRelation.Materialize()
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
		return nil, fmt.Errorf("Table name must be in the format `one/two/three`, have: %s",
			is.TableName)
	}

	fi, err := is.CatalogDirectory.GetLatestTimeBucketInfoFromKey(targetMK)
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
		// Add the Epoch column name
		targetColumnNames = append(targetColumnNames, "Epoch")
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
	indexTime, err := inputColumnSeries.GetTime()

	// Columns are matched - Now project out all but the target column names
	inputColumnSeries.Project(targetColumnNames)
	/*
		Write the data
	*/
	tgc := executor.ThisInstance.TXNPipe
	catDir := is.CatalogDirectory
	wal := executor.ThisInstance.WALFile
	tbi, err := catDir.GetLatestTimeBucketInfoFromKey(targetMK)
	if err != nil {
		return nil, err
	}
	writer, err := executor.NewWriter(tbi, tgc, catDir)
	if err != nil {
		return nil, err
	}
	/*
		Serialize the Column Series for writing, with the targetDSV controlling projections and coercion
	*/
	data, _ := io.SerializeColumnsToRows(inputColumnSeries, targetDSV, true)
	if data == nil {
		return nil, fmt.Errorf("Unable to pre-process data for insertion")
	}

	writer.WriteRecords(indexTime, data, targetDSV)
	wal.RequestFlush()

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
