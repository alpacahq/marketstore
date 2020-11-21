package io

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

func isIterable(i interface{}) bool {
	kind := reflect.TypeOf(i).Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

// CoerceColumnType replaces the data type of values in a column that has the specified name to the specified elementType
func (cs *ColumnSeries) CoerceColumnType(columnName string, elementType EnumElementType) (err error) {
	if elementType == BOOL || elementType == STRING {
		return fmt.Errorf("Can not cast to boolean or string")
	}

	iCol := cs.GetByName(columnName)
	if !isIterable(iCol) {
		return errors.New("bug! column values should be a slice or array")
	}

	columnValues := reflect.ValueOf(iCol)

	switch elementType.Kind() {
	case reflect.Int8:
		newCol := make([]byte, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = byte(toInt(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Int16:
		newCol := make([]int16, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = int16(toInt(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Int32:
		newCol := make([]int32, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = int32(toInt(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Int64:
		newCol := make([]int64, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = toInt(columnValues.Index(i))
		}
		cs.columns[columnName] = newCol
	case reflect.Uint8:
		newCol := make([]uint8, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = uint8(toUint(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Uint16:
		newCol := make([]uint16, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = uint16(toUint(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Uint32:
		newCol := make([]uint32, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = uint32(toUint(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Uint64:
		newCol := make([]uint64, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = uint64(toUint(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Float32:
		newCol := make([]float32, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = float32(toFloat(columnValues.Index(i)))
		}
		cs.columns[columnName] = newCol
	case reflect.Float64:
		newCol := make([]float64, columnValues.Len())
		for i := 0; i < columnValues.Len(); i++ {
			newCol[i] = toFloat(columnValues.Index(i))
		}
		cs.columns[columnName] = newCol
	}
	return nil
}

var group = map[string]map[reflect.Kind]struct{}{
	"float": {reflect.Float32: {}, reflect.Float64: {}},
	"int":   {reflect.Int: {}, reflect.Int8: {}, reflect.Int16: {}, reflect.Int32: {}, reflect.Int64: {}},
	"uint":  {reflect.Uint: {}, reflect.Uint8: {}, reflect.Uint16: {}, reflect.Uint32: {}, reflect.Uint64: {}},
}

// toFloat casts a float or int value to float64.
func toFloat(v reflect.Value) float64 {
	if _, found := group["float"][v.Kind()]; found {
		return v.Float()
	}
	if _, found := group["int"][v.Kind()]; found {
		return float64(v.Int())
	}
	return float64(v.Uint())
}

// toInt casts a int or float value to int64.
func toInt(v reflect.Value) int64 {
	if _, found := group["int"][v.Kind()]; found {
		return v.Int()
	}
	if _, found := group["float"][v.Kind()]; found {
		return int64(v.Float())
	}
	return int64(v.Uint())
}

// toUint casts a int or float value to uint64.
func toUint(v reflect.Value) uint64 {
	if _, found := group["uint"][v.Kind()]; found {
		return v.Uint()
	}
	if _, found := group["int"][v.Kind()]; found {
		return uint64(v.Int())
	}
	return uint64(v.Float())
}
