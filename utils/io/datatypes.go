package io

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

//go:generate stringer -type=EnumElementType,EnumRecordType datatypes.go byteconversions.go

/*
#include "utilityfuncs.h"
#cgo CFLAGS: -O3 -std=c99
*/
import "C"

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
	EPOCH
	BYTE
	BOOL
	NONE
	STRING
	INT16
	UINT8
	UINT16
	UINT32
	UINT64
)

var (
	attributeMap = map[EnumElementType]struct {
		typ    reflect.Kind
		name   string
		size   int
		typeOf reflect.Type
	}{
		FLOAT32: {reflect.Float32, "float32", 4, reflect.TypeOf(float32(0))},
		INT32:   {reflect.Int32, "int32", 4, reflect.TypeOf(int32(0))},
		FLOAT64: {reflect.Float64, "float64", 8, reflect.TypeOf(float64(0))},
		INT64:   {reflect.Int64, "int64", 8, reflect.TypeOf(int64(0))},
		EPOCH:   {reflect.Int64, "epoch", 8, reflect.TypeOf(int64(0))},
		BYTE:    {reflect.Int8, "byte", 1, reflect.TypeOf(byte(0))},
		BOOL:    {reflect.Bool, "bool", 1, reflect.TypeOf(bool(false))},
		NONE:    {reflect.Invalid, "none", 0, reflect.TypeOf(byte(0))},
		STRING:  {reflect.String, "string", 0, reflect.TypeOf("")},
		INT16:   {reflect.Int16, "int16", 2, reflect.TypeOf(int16(0))},
		UINT8:   {reflect.Uint8, "uint8", 1, reflect.TypeOf(uint8(0))},
		UINT16:  {reflect.Uint16, "uint16", 2, reflect.TypeOf(uint16(0))},
		UINT32:  {reflect.Uint32, "uint32", 4, reflect.TypeOf(uint32(0))},
		UINT64:  {reflect.Uint64, "uint64", 8, reflect.TypeOf(uint64(0))},
	}
)

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

// ByteSliceAt returns a byte representation of the element in the original type
// slice at index position.
func (e EnumElementType) ByteSliceAt(sliceOf interface{}, index int) (bs []byte) {
	bs = SwapSliceData(sliceOf, byte(0)).([]byte)
	return e.SliceInBytesAt(bs, index)
}

func (e EnumElementType) SliceOf(length int) (sliceOf interface{}) {
	typeOf := attributeMap[e].typeOf
	return reflect.MakeSlice(typeOf, length, length)
}

func (e EnumElementType) ConvertByteSliceInto(data []byte) interface{} {
	switch e {
	case FLOAT32:
		return SwapSliceByte(data, float32(0)).([]float32)
	case INT32:
		return SwapSliceByte(data, int32(0)).([]int32)
	case FLOAT64:
		return SwapSliceByte(data, float64(0)).([]float64)
	case INT64, EPOCH:
		return SwapSliceByte(data, int64(0)).([]int64)
	case BYTE, BOOL:
		return SwapSliceByte(data, int8(0)).([]int8)
	case INT16:
		return SwapSliceByte(data, int16(0)).([]int16)
	case STRING:
		return SwapSliceByte(data, string(0)).([]string)
	case UINT8:
		return SwapSliceByte(data, uint8(0)).([]uint8)
	case UINT16:
		return SwapSliceByte(data, uint16(0)).([]uint16)
	case UINT32:
		return SwapSliceByte(data, uint32(0)).([]uint32)
	case UINT64:
		return SwapSliceByte(data, uint64(0)).([]uint64)
	}
	return nil
}

func GetElementType(datum interface{}) EnumElementType {
	// O(N)
	value := reflect.ValueOf(datum)
	kind := value.Kind()
	switch kind {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
		kind = reflect.TypeOf(datum).Elem().Kind()
	}
	switch kind {
	case reflect.Struct, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return NONE
	default:
		/*
			We need to iterate over this map in order of the Enum
		*/
		for i := 0; i <= int(UINT64); i++ {
			e := EnumElementType(i)
			el := attributeMap[e]
			if el.typ == kind {
				return e
			}
		}
	}
	return NONE
}

type CandleAttributes uint8

/*
- if None, then there are no candle attributes - not a candle
- if both HASFLOAT32 and HASFLOAT64 are set, the 64-bit versions of OHLC are named using a "64" after the column name
- if only one of HASFLOAT32 and HASFLOAT64 are set, the OHLC have names: "open", "high", etc
*/
const (
	None       CandleAttributes = 0x0       // Default - not a candle
	ISCANDLE                    = 1 << iota // Is a candle - continuum data representation (not a tick)
	OHLC                                    // Has "Open, High, Low, Close" data in the candle
	OHLCV                                   // Has "Open, High, Low, Close" and "Volume" data in the candle
	HASFLOAT32                              // 32-bit version available
	HASFLOAT64                              // 64-bit version available
)

func (cat *CandleAttributes) AddOption(option CandleAttributes) {
	*cat |= option
}
func (cat *CandleAttributes) DelOption(option CandleAttributes) {
	*cat &= ^option
}
func (cat *CandleAttributes) IsSet(checkOption ...CandleAttributes) bool {
	/*
		Returns true if all supplied options are set
	*/
	for _, co := range checkOption {
		if (*cat)&co != co {
			return false
		}
	}
	return true
}
func (cat *CandleAttributes) AnySet(checkOption ...CandleAttributes) bool {
	/*
		Returns true if any of the supplied options are set
	*/
	for _, co := range checkOption {
		if (*cat)&co == co {
			return true
		}
	}
	return false
}

type DirectionEnum uint8

const (
	FIRST DirectionEnum = iota
	LAST
)

/*
===========================================================================================
Utility functions
===========================================================================================
*/

func getFloat32Column(offset, reclen, nrecs int, data []byte) (col []float32) {
	//	fmt.Println("offset, reclen, nrecs: ", offset, reclen, nrecs)
	col = make([]float32, nrecs)
	if nrecs == 0 {
		return col
	}
	arg1 := (*C.char)(unsafe.Pointer(&data[0]))
	arg6 := (*C.float)(unsafe.Pointer(&col[0]))
	C.wordCopyFloat32(arg1, C.int(offset), C.int(reclen), C.int(nrecs), arg6)
	return col
}
func getFloat64Column(offset, reclen, nrecs int, data []byte) (col []float64) {
	col = make([]float64, nrecs)
	if nrecs == 0 {
		return col
	}
	arg1 := (*C.char)(unsafe.Pointer(&data[0]))
	arg6 := (*C.double)(unsafe.Pointer(&col[0]))
	C.wordCopyFloat64(arg1, C.int(offset), C.int(reclen), C.int(nrecs), arg6)
	return col
}
func getInt32Column(offset, reclen, nrecs int, data []byte) (col []int32) {
	col = make([]int32, nrecs)
	if nrecs == 0 {
		return col
	}
	arg1 := (*C.char)(unsafe.Pointer(&data[0]))
	arg6 := (*C.int32_t)(unsafe.Pointer(&col[0]))
	C.wordCopyInt32(arg1, C.int(offset), C.int(reclen), C.int(nrecs), arg6)
	return col
}
func getInt64Column(offset, reclen, nrecs int, data []byte) (col []int64) {
	col = make([]int64, nrecs)
	if nrecs == 0 {
		return col
	}
	arg1 := (*C.char)(unsafe.Pointer(&data[0]))
	arg6 := (*C.int64_t)(unsafe.Pointer(&col[0]))
	C.wordCopyInt64(arg1, C.int(offset), C.int(reclen), C.int(nrecs), arg6)
	return col
}
func getByteColumn(offset, reclen, nrecs int, data []byte) (col []byte) {
	col = make([]byte, nrecs)
	if nrecs == 0 {
		return col
	}
	for i := 0; i < nrecs; i++ {
		col[i] = data[i*reclen+offset]
	}
	return col
}

func CreateSliceFromSliceOfInterface(input []interface{}, typ EnumElementType) (i_output interface{}, err error) {
	switch typ {
	case FLOAT32:
		output := []float32{}
		for _, i_elem := range input {
			switch val := i_elem.(type) {
			case float32:
				output = append(output, float32(val))
			case float64:
				output = append(output, float32(val))
			default:
				return nil, fmt.Errorf("non coercible type")
			}
		}
		i_output = output
	case INT32:
		output := []int32{}
		for _, i_elem := range input {
			switch val := i_elem.(type) {
			case byte:
				output = append(output, int32(val))
			case int8:
				output = append(output, int32(val))
			case int16:
				output = append(output, int32(val))
			case uint16:
				output = append(output, int32(val))
			case int32:
				output = append(output, int32(val))
			case uint32:
				output = append(output, int32(val))
			case int64:
				output = append(output, int32(val))
			case uint64:
				output = append(output, int32(val))
			default:
				return nil, fmt.Errorf("non coercible type")
			}
		}
		i_output = output
	case FLOAT64:
		output := []float64{}
		for _, i_elem := range input {
			switch val := i_elem.(type) {
			case float32:
				output = append(output, float64(val))
			case float64:
				output = append(output, float64(val))
			default:
				return nil, fmt.Errorf("non coercible type")
			}
		}
		i_output = output
	case INT64, EPOCH:
		output := []int64{}
		for _, i_elem := range input {
			switch val := i_elem.(type) {
			case byte:
				output = append(output, int64(val))
			case int8:
				output = append(output, int64(val))
			case int16:
				output = append(output, int64(val))
			case uint16:
				output = append(output, int64(val))
			case int32:
				output = append(output, int64(val))
			case uint32:
				output = append(output, int64(val))
			case int64:
				output = append(output, int64(val))
			case uint64:
				output = append(output, int64(val))
			default:
				return nil, fmt.Errorf("non coercible type")
			}
		}
		i_output = output
	case BYTE:
		output := []byte{}
		for _, i_elem := range input {
			switch val := i_elem.(type) {
			case byte:
				output = append(output, byte(val))
			case int8:
				output = append(output, byte(val))
			case int16:
				output = append(output, byte(val))
			case uint16:
				output = append(output, byte(val))
			case int32:
				output = append(output, byte(val))
			case uint32:
				output = append(output, byte(val))
			case int64:
				output = append(output, byte(val))
			case uint64:
				output = append(output, byte(val))
			default:
				return nil, fmt.Errorf("non coercible type")
			}
		}
		i_output = output
	}
	return i_output, nil
}
