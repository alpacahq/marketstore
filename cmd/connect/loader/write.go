package loader

import (
	"fmt"
	"github.com/alpacahq/marketstore/utils/io"
	"strconv"
)

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
