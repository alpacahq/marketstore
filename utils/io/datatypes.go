package io

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

//go:generate stringer -type=EnumElementType,EnumRecordType datatypes.go byteconversions.go

type EnumRecordType int8

const (
	FIXED EnumRecordType = iota
	VARIABLE
	NOTYPE
)

func EnumRecordTypeByName(name string) EnumRecordType {
	name = strings.ToLower(name)
	switch name {
	case "fixed":
		return FIXED
	case "variable":
		return VARIABLE
	default:
		return NOTYPE
	}
}

type EnumElementType byte

/*
NOTE: The ordering of this enum must match the File Format order

We define our own types here instead of using the (excellent!) built-in Go type system for the primary reason
that we are serializing data to files and so need to have a (very!) stable on-disk representation that matches
the processing we do internally.
*/
const (
	FLOAT32 EnumElementType = iota
	INT32
	FLOAT64
	INT64
	BYTE
	BOOL
	NONE
	STRING
	INT16
	UINT8
	UINT16
	UINT32
	UINT64
	STRING16
)

var attributeMap = map[EnumElementType]struct {
	typ    reflect.Kind
	name   string
	size   int
	typeOf reflect.Type
}{
	FLOAT32:  {reflect.Float32, "float32", 4, reflect.TypeOf(float32(0))},
	INT32:    {reflect.Int32, "int32", 4, reflect.TypeOf(int32(0))},
	FLOAT64:  {reflect.Float64, "float64", 8, reflect.TypeOf(float64(0))},
	INT64:    {reflect.Int64, "int64", 8, reflect.TypeOf(int64(0))},
	BYTE:     {reflect.Int8, "byte", 1, reflect.TypeOf(byte(0))},
	BOOL:     {reflect.Bool, "bool", 1, reflect.TypeOf(false)},
	NONE:     {reflect.Invalid, "none", 0, reflect.TypeOf(byte(0))},
	STRING:   {reflect.String, "string", 0, reflect.TypeOf("")},
	INT16:    {reflect.Int16, "int16", 2, reflect.TypeOf(int16(0))},
	UINT8:    {reflect.Uint8, "uint8", 1, reflect.TypeOf(uint8(0))},
	UINT16:   {reflect.Uint16, "uint16", 2, reflect.TypeOf(uint16(0))},
	UINT32:   {reflect.Uint32, "uint32", 4, reflect.TypeOf(uint32(0))},
	UINT64:   {reflect.Uint64, "uint64", 8, reflect.TypeOf(uint64(0))},
	STRING16: {reflect.Array, "string16", 64, reflect.TypeOf([16]rune{})},
}

func EnumElementTypeFromName(name string) EnumElementType {
	// O(N)
	for key, el := range attributeMap {
		if strings.EqualFold(name, el.name) {
			return key
		}
	}
	return NONE
}

func (e EnumElementType) TypeOf() reflect.Type {
	return attributeMap[e].typeOf
}

func (e EnumElementType) Kind() reflect.Kind {
	return attributeMap[e].typ
}

func (e EnumElementType) Size() int {
	return attributeMap[e].size
}

// SliceInBytesAt returns a byte representation of the element at
// index position of the original type slice, but takes byte representation
// of the original slice.  The caller can use this over ByteSliceAt() to
// avoid repeated internal SwapSliceData calls.
func (e EnumElementType) SliceInBytesAt(bs []byte, index int) []byte {
	offset := index * e.Size()
	return bs[offset : offset+e.Size()]
}

func (e EnumElementType) SliceOf(length int) (sliceOf interface{}) {
	typeOf := attributeMap[e].typeOf
	return reflect.MakeSlice(typeOf, length, length)
}

func (e EnumElementType) ConvertByteSliceInto(data []byte) (interface{}, error) {
	switch e {
	case FLOAT32:
		if val, err := SwapSliceByte(data, float32(0)); err == nil {
			if slc, ok := val.([]float32); ok {
				return slc, nil
			}
		}
	case INT32:
		if val, err := SwapSliceByte(data, int32(0)); err == nil {
			if slc, ok := val.([]int32); ok {
				return slc, nil
			}
		}
	case FLOAT64:
		if val, err := SwapSliceByte(data, float64(0)); err == nil {
			if slc, ok := val.([]float64); ok {
				return slc, nil
			}
		}
	case INT64:
		if val, err := SwapSliceByte(data, int64(0)); err == nil {
			if slc, ok := val.([]int64); ok {
				return slc, nil
			}
		}
	case BYTE, BOOL:
		if val, err := SwapSliceByte(data, int8(0)); err == nil {
			if slc, ok := val.([]int8); ok {
				return slc, nil
			}
		}
	case INT16:
		if val, err := SwapSliceByte(data, int16(0)); err == nil {
			if slc, ok := val.([]int16); ok {
				return slc, nil
			}
		}
	case STRING:
		if val, err := SwapSliceByte(data, fmt.Sprint(0)); err == nil {
			if slc, ok := val.([]string); ok {
				return slc, nil
			}
		}
	case UINT8:
		if val, err := SwapSliceByte(data, uint8(0)); err == nil {
			if slc, ok := val.([]uint8); ok {
				return slc, nil
			}
		}
	case UINT16:
		if val, err := SwapSliceByte(data, uint16(0)); err == nil {
			if slc, ok := val.([]uint16); ok {
				return slc, nil
			}
		}
	case UINT32:
		if val, err := SwapSliceByte(data, uint32(0)); err == nil {
			if slc, ok := val.([]uint32); ok {
				return slc, nil
			}
		}
	case UINT64:
		if val, err := SwapSliceByte(data, uint64(0)); err == nil {
			if slc, ok := val.([]uint64); ok {
				return slc, nil
			}
		}
	case STRING16:
		if val, err := SwapSliceByte(data, [16]rune{}); err == nil {
			if slc, ok := val.([][16]rune); ok {
				return slc, nil
			}
		}
	default:
		return nil, errors.New("unknown column type specified for ConvertByteSliceInfo")
	}
	return nil, errors.New("unexpected buffer specified for ConvertByteSliceInto")
}

func GetElementType(datum interface{}) EnumElementType {
	// O(N)
	value := reflect.ValueOf(datum)
	kind := value.Kind()
	switch kind {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
		kind = reflect.TypeOf(datum).Elem().Kind()
	default:
		// pass
	}
	switch kind {
	case reflect.Struct, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return NONE
	default:
		/*
			We need to iterate over this map in order of the Enum
		*/
		for i := 0; i <= int(STRING16); i++ {
			e := EnumElementType(i)
			el := attributeMap[e]
			if el.typ != kind {
				continue
			}
			// el.typ == kind
			// need to check the length too in case of String type (=[]rune = array type),
			if kind == reflect.Array {
				if el.size == int(value.Type().Elem().Size()) {
					return e
				}
			} else {
				return e
			}
		}
	}
	return NONE
}

type DirectionEnum uint8

const (
	// limit_from_start=true -> FIRST
	// limit_from_start=false -> LAST (default).
	FIRST DirectionEnum = iota
	LAST
)

/*
===========================================================================================
Utility functions
===========================================================================================
*/

func getFloat32Column(offset, reclen, nrecs int, data []byte) (col []float32) {
	col = make([]float32, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToFloat32(data[cursor : cursor+4])
		cursor += reclen
	}
	return col
}

func getFloat64Column(offset, reclen, nrecs int, data []byte) (col []float64) {
	col = make([]float64, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToFloat64(data[cursor : cursor+8])
		cursor += reclen
	}
	return col
}

func getInt16Column(offset, reclen, nrecs int, data []byte) (col []int16) {
	col = make([]int16, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToInt16(data[cursor : cursor+2])
		cursor += reclen
	}
	return col
}

func getInt32Column(offset, reclen, nrecs int, data []byte) (col []int32) {
	col = make([]int32, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToInt32(data[cursor : cursor+4])
		cursor += reclen
	}
	return col
}

func getInt64Column(offset, reclen, nrecs int, data []byte) (col []int64) {
	col = make([]int64, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToInt64(data[cursor : cursor+8])
		cursor += reclen
	}
	return col
}

func getUInt8Column(offset, reclen, nrecs int, data []byte) (col []uint8) {
	col = make([]uint8, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = data[cursor]
		cursor += reclen
	}
	return col
}

func getUInt16Column(offset, reclen, nrecs int, data []byte) (col []uint16) {
	col = make([]uint16, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToUInt16(data[cursor : cursor+2])
		cursor += reclen
	}
	return col
}

func getUInt32Column(offset, reclen, nrecs int, data []byte) (col []uint32) {
	col = make([]uint32, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToUInt32(data[cursor : cursor+4])
		cursor += reclen
	}
	return col
}

func getUInt64Column(offset, reclen, nrecs int, data []byte) (col []uint64) {
	col = make([]uint64, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = ToUInt64(data[cursor : cursor+8])
		cursor += reclen
	}
	return col
}

func getString16Column(offset, reclen, nrecs int, data []byte) (col [][16]rune) {
	col = make([][16]rune, nrecs)
	if nrecs == 0 {
		return col
	}
	cursor := offset
	for i := 0; i < nrecs; i++ {
		subCursor := cursor
		for k := 0; k < 16; k++ {
			col[i][k] = ToRune(data[subCursor : subCursor+4]) // 1 rune = 4 byte
			subCursor += 4
		}
		cursor += reclen
	}
	return col
}

func getByteColumn(offset, reclen, nrecs int, data []byte) (col []byte) {
	col = make([]byte, nrecs)
	if nrecs == 0 {
		return col
	}

	cursor := offset
	for i := 0; i < nrecs; i++ {
		col[i] = data[cursor]
		cursor += reclen
	}
	return col
}
