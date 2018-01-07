package io

import (
	"fmt"
	"reflect"
)

func (cs *ColumnSeries) CoerceColumnType(ds DataShape) (err error) {
	//TODO: Make this generic and maintainable
	if ds.Type == BOOL || ds.Type == STRING {
		return fmt.Errorf("Can not cast to boolean or string")
	}
	i_col := cs.GetByName(ds.Name)

	switch col := i_col.(type) {
	case []int:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []int8:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []float32:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []int32:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []float64:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []int64:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []int16:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []uint8:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []uint16:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []uint32:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	case []uint64:
		switch ds.Type.Kind() {
		case reflect.Int:
			var newCol []int
			for _, value := range col {
				newCol = append(newCol, int(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int8:
			var newCol []int8
			for _, value := range col {
				newCol = append(newCol, int8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float32:
			var newCol []float32
			for _, value := range col {
				newCol = append(newCol, float32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int32:
			var newCol []int32
			for _, value := range col {
				newCol = append(newCol, int32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Float64:
			var newCol []float64
			for _, value := range col {
				newCol = append(newCol, float64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int64:
			var newCol []int64
			for _, value := range col {
				newCol = append(newCol, int64(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Int16:
			var newCol []int16
			for _, value := range col {
				newCol = append(newCol, int16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint8:
			var newCol []uint8
			for _, value := range col {
				newCol = append(newCol, uint8(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint16:
			var newCol []uint16
			for _, value := range col {
				newCol = append(newCol, uint16(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint32:
			var newCol []uint32
			for _, value := range col {
				newCol = append(newCol, uint32(value))
			}
			cs.columns[ds.Name] = newCol
		case reflect.Uint64:
			var newCol []uint64
			for _, value := range col {
				newCol = append(newCol, uint64(value))
			}
			cs.columns[ds.Name] = newCol
		}
	}
	return nil
}

func (cs *ColumnSeries) RestrictViaBitmap(bitmap []bool) (err error) {
	var bitmapValidLength int
	for _, val := range bitmap {
		if !val {
			bitmapValidLength++
		}
	}
	for _, key := range cs.orderedNames {
		i_col := cs.columns[key]
		switch col := i_col.(type) {
		case []int:
			newCol := make([]int, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []int8:
			newCol := make([]int8, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []float32:
			newCol := make([]float32, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []int32:
			newCol := make([]int32, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []float64:
			newCol := make([]float64, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []int64:
			newCol := make([]int64, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []int16:
			newCol := make([]int16, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []uint8:
			newCol := make([]uint8, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []uint16:
			newCol := make([]uint16, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []uint32:
			newCol := make([]uint32, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		case []uint64:
			newCol := make([]uint64, bitmapValidLength)
			var newColCursor int
			for i, val := range bitmap {
				if !val { // If the bitmap is true, remove the value
					newCol[newColCursor] = col[i]
					newColCursor++
				}
			}
			if err := cs.Replace(key, newCol); err != nil {
				return err
			}
		}
	}
	return nil
}
