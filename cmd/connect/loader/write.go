package loader

import (
	"fmt"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

// Configuration is constructed from the control file
// that specifies the formatting of the csv data.
type Configuration struct {
	FirstRowHasColumnNames bool     `yaml:"firstRowHasColumnNames"`
	TimeFormat             string   `yaml:"timeFormat"`
	Timezone               string   `yaml:"timeZone"`
	ColumnNameMap          []string `yaml:"columnNameMap"`
}

// WriteChunk writes data to the database.
func WriteChunk(dbWriter *executor.Writer, dataShapes []io.DataShape, dbKey io.TimeBucketKey, columnIndex []int, csvDataChunk [][]string, conf *Configuration) (start, end time.Time) {

	epochCol, nanosCol := readTimeColumns(csvDataChunk, columnIndex, conf)
	if epochCol == nil {
		fmt.Println("Error building time columns from csv data")
		return
	}

	csmInit := io.NewColumnSeriesMap()
	csmInit.AddColumn(dbKey, "Epoch", epochCol)
	csm := columnSeriesMapFromCSVData(csmInit, dbKey, csvDataChunk, columnIndex[2:], dataShapes)
	csmInit.AddColumn(dbKey, "Nanoseconds", nanosCol)

	dsMap := make(map[io.TimeBucketKey][]io.DataShape)
	dsMap[dbKey] = dataShapes
	rsMap := csm.ToRowSeriesMap(dsMap)
	rs := rsMap[dbKey]
	if rs.GetNumRows() != len(csvDataChunk) {
		fmt.Println("Error obtaining rows from CSV file - not enough rows converted")
		fmt.Println("Expected: ", len(csvDataChunk), " Got: ", rs.GetNumRows())
		for _, cs := range csm {
			fmt.Println("ColNames: ", cs.GetColumnNames())
		}
		return
	}
	fmt.Printf("beginning to write %d records...", rs.GetNumRows())
	indexTime := make([]time.Time, 0)
	for i := 0; i < rs.GetNumRows(); i++ {
		indexTime = append(indexTime, time.Unix(epochCol[i], int64(nanosCol[i])).UTC())
	}
	dbWriter.WriteRecords(indexTime, rs.GetData())

	executor.ThisInstance.WALFile.RequestFlush()
	fmt.Printf("Done.\n")

	start = time.Unix(epochCol[0], 0).UTC()
	end = time.Unix(epochCol[len(epochCol)-1], 0).UTC()

	return start, end
}

func columnSeriesMapFromCSVData(csmInit io.ColumnSeriesMap, key io.TimeBucketKey, csvRows [][]string, columnIndex []int,
	dataShapes []io.DataShape) (csm io.ColumnSeriesMap) {

	if csmInit == nil {
		csm = io.NewColumnSeriesMap()
	} else {
		csm = csmInit
	}
	for i, shape := range dataShapes {
		index := columnIndex[i]
		if index != 0 {
			/*
				We skip the first column, as it's the Epoch and we parse that independently
			*/
			switch shape.Type {
			case io.FLOAT32:
				col, err := getFloat32ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.FLOAT64:
				col, err := getFloat64ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT32:
				col, err := getInt32ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT64:
				col, err := getInt64ColumnFromCSVRows(csvRows, index)
				if columnError(err, shape.Name) {
					return nil
				}
				csm.AddColumn(key, shape.Name, col)
			}
		}
	}
	return csm
}

func columnError(err error, name string) bool {
	if err != nil {
		fmt.Printf("Error obtaining column \"%s\" from csv data\n", name)
		return true
	}
	return false
}

func getFloat32ColumnFromCSVRows(csvRows [][]string, index int) (col []float32, err error) {
	col = make([]float32, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseFloat(row[index], 32)
		if err != nil {
			return nil, err
		}
		col[i] = float32(val)
	}
	return col, nil
}

func getFloat64ColumnFromCSVRows(csvRows [][]string, index int) (col []float64, err error) {
	col = make([]float64, len(csvRows))
	for i, row := range csvRows {
		col[i], err = strconv.ParseFloat(row[index], 64)
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}

func getInt32ColumnFromCSVRows(csvRows [][]string, index int) (col []int32, err error) {
	col = make([]int32, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseInt(row[index], 10, 32)
		if err != nil {
			return nil, err
		}
		col[i] = int32(val)
	}
	return col, nil
}

func getInt64ColumnFromCSVRows(csvRows [][]string, index int) (col []int64, err error) {
	col = make([]int64, len(csvRows))
	for i, row := range csvRows {
		col[i], err = strconv.ParseInt(row[index], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}
