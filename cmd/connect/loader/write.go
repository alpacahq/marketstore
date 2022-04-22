package loader

import (
	"fmt"
	"strconv"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func columnSeriesMapFromCSVData(csmInit io.ColumnSeriesMap, key io.TimeBucketKey, csvRows [][]string, columnIndex []int,
	dataShapes []io.DataShape) (csm io.ColumnSeriesMap, err error) {
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
			case io.STRING:
				col := getStringColumnFromCSVRows(csvRows, index)
				csm.AddColumn(key, shape.Name, col)
			case io.FLOAT32:
				col, err := getFloat32ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.FLOAT64:
				col, err := getFloat64ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.BYTE:
				col, err := getInt8ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT16:
				col, err := getInt16ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT32:
				col, err := getInt32ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.INT64:
				col, err := getInt64ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.UINT8:
				col, err := getUInt8ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.UINT16:
				col, err := getUInt16ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.UINT32:
				col, err := getUInt32ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.UINT64:
				col, err := getUInt64ColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			case io.STRING16:
				col := getString16ColumnFromCSVRows(csvRows, index)
				csm.AddColumn(key, shape.Name, col)
			case io.BOOL:
				col, err := getBoolColumnFromCSVRows(csvRows, index)
				if err != nil {
					return nil, fmt.Errorf("error obtaining column \"%s\" from csv data", shape.Name)
				}
				csm.AddColumn(key, shape.Name, col)
			default:
				return nil, fmt.Errorf("unknown column type.rror obtaining column \"%s\" from csv data",
					shape.Name,
				)
			}
		}
	}
	return csm, nil
}

func getBoolColumnFromCSVRows(csvRows [][]string, index int) (col []bool, err error) {
	col = make([]bool, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseBool(row[index])
		if err != nil {
			return nil, err
		}
		col[i] = val
	}
	return col, nil
}

func getStringColumnFromCSVRows(csvRows [][]string, index int) (col []string) {
	col = make([]string, len(csvRows))
	for i, row := range csvRows {
		col[i] = row[index]
	}
	return col
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

func getInt8ColumnFromCSVRows(csvRows [][]string, index int) (col []int8, err error) {
	col = make([]int8, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseInt(row[index], 10, 8)
		if err != nil {
			return nil, err
		}
		col[i] = int8(val)
	}
	return col, nil
}

func getInt16ColumnFromCSVRows(csvRows [][]string, index int) (col []int16, err error) {
	col = make([]int16, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseInt(row[index], 10, 16)
		if err != nil {
			return nil, err
		}
		col[i] = int16(val)
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

func getUInt8ColumnFromCSVRows(csvRows [][]string, index int) (col []uint8, err error) {
	col = make([]uint8, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseUint(row[index], 10, 8)
		if err != nil {
			return nil, err
		}
		col[i] = uint8(val)
	}
	return col, nil
}

func getUInt16ColumnFromCSVRows(csvRows [][]string, index int) (col []uint16, err error) {
	col = make([]uint16, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseUint(row[index], 10, 16)
		if err != nil {
			return nil, err
		}
		col[i] = uint16(val)
	}
	return col, nil
}

func getUInt32ColumnFromCSVRows(csvRows [][]string, index int) (col []uint32, err error) {
	col = make([]uint32, len(csvRows))
	for i, row := range csvRows {
		val, err := strconv.ParseUint(row[index], 10, 32)
		if err != nil {
			return nil, err
		}
		col[i] = uint32(val)
	}
	return col, nil
}

func getUInt64ColumnFromCSVRows(csvRows [][]string, index int) (col []uint64, err error) {
	col = make([]uint64, len(csvRows))
	for i, row := range csvRows {
		col[i], err = strconv.ParseUint(row[index], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}

const String16RuneSize = 16

func getString16ColumnFromCSVRows(csvRows [][]string, index int) (col [][String16RuneSize]rune) {
	col = make([][String16RuneSize]rune, len(csvRows))
	for i, row := range csvRows {
		if len([]rune(row[index])) > String16RuneSize {
			log.Warn(fmt.Sprintf("too long string column (>16chars):%v", row[index]))

			copy(col[i][:], []rune(row[index][0:String16RuneSize]))
		} else {
			copy(col[i][:], []rune(row[index][0:len(row[index])]))
		}
	}
	return col
}
