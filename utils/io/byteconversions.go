package io

import (
	"errors"
	"reflect"
	"unsafe"
)

// MValue is a *copy* of the "Value" struct inside the reflect package.
type MValue struct {
	typ uintptr
	Ptr unsafe.Pointer
}

// SwapSliceByte converts a byte slice of the type into a slice of the target type
// without copying each value in the slice.
func SwapSliceByte(srcByteSlice, targetType interface{}) (interface{}, error) {
	buffer, ok := srcByteSlice.([]byte)
	if !ok {
		return nil, errors.New("failed to cast source data to a byte slice")
	}

	structValue := reflect.ValueOf(targetType)
	structType := structValue.Type()
	structSize := structType.Size()
	// structSize := binary.Size(targetType)
	structSliceType := reflect.SliceOf(structType)

	Len := len(buffer) / int(structSize)
	Cap := Len
	structSlice := reflect.MakeSlice(structSliceType, Len, Cap)

	// Set the new slice header data to that of the byte slice
	(*reflect.SliceHeader)((*MValue)(unsafe.Pointer(&structSlice)).Ptr).Data =
		(*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Data

	return structSlice.Interface(), nil
}

func ToUint8(b []byte) uint8 {
	return b[0]
}

func ToInt8(b []byte) int8 {
	return *(*int8)(unsafe.Pointer(&b[0]))
}

func ToInt16(b []byte) int16 {
	return *(*int16)(unsafe.Pointer(&b[0]))
}

func ToUInt16(b []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&b[0]))
}

func ToInt32(b []byte) int32 {
	return *(*int32)(unsafe.Pointer(&b[0]))
}

func ToUInt32(b []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&b[0]))
}

func ToInt64(b []byte) int64 {
	return *(*int64)(unsafe.Pointer(&b[0]))
}

func ToUInt64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

func ToFloat32(b []byte) float32 {
	return *(*float32)(unsafe.Pointer(&b[0]))
}

func ToFloat64(b []byte) float64 {
	return *(*float64)(unsafe.Pointer(&b[0]))
}

func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ToRune(b []byte) rune {
	return *(*rune)(unsafe.Pointer(&b[0]))
}

// SwapSliceData generically converts a slice of the type into a slice of the target type
// without copying each value in the slice.
func SwapSliceData(srcSlice, targetType interface{}) interface{} {
	src := reflect.ValueOf(srcSlice)
	srcLen := src.Len()
	srcElementType := src.Type().Elem()
	// .Size() considers the array values. For example, if srcElementType=[16]rune, Size() returns 64.
	srcElementTypeSize := srcElementType.Size()
	//	fmt.Printf("LeftType: %s LeftLen: %d LeftSize: %d \n",srcElementType, srcLen, srcElementTypeSize)

	targetValue := reflect.ValueOf(targetType)
	targetValueType := targetValue.Type()
	targetSize := targetValueType.Size()
	targetLen := (srcLen * int(srcElementTypeSize)) / int(targetSize)

	// targetSize := binary.Size(target_type)
	targetSliceType := reflect.SliceOf(targetValueType)

	//	fmt.Printf("LeftType: %s LeftLen: %d LeftSize: %d RightSize: %d\n",
	//  	srcElementType, leftLen, srcElementTypeSize, targetSize)
	targetCap := targetLen
	targetSlice := reflect.MakeSlice(targetSliceType, targetLen, targetCap)

	// Set the data pointer of the right slice equal to that of the left
	(*reflect.SliceHeader)((*MValue)(unsafe.Pointer(&targetSlice)).Ptr).Data =
		(*reflect.SliceHeader)((*MValue)(unsafe.Pointer(&src)).Ptr).Data

	return targetSlice.Interface()
}

// CastToByteSlice casts sliceData's memory chunk to a byte slice without copy.
func CastToByteSlice(sliceData interface{}) []byte {
	sliceValue := reflect.ValueOf(sliceData)
	sliceLen := sliceValue.Len()
	elemType := sliceValue.Type().Elem()
	elemSize := elemType.Size()

	bufLen := sliceLen * int(elemSize)
	buffer := make([]byte, 0)
	bufHeader := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	bufHeader.Len = bufLen
	bufHeader.Cap = bufLen
	bufHeader.Data = sliceValue.Pointer()

	return buffer
}

// DataToByteSlice takes a primary (non slice, non pointer) type and returns a []byte of the base type data.
func DataToByteSlice(srcData interface{}) []byte {
	value := reflect.ValueOf(srcData)
	size := int(value.Type().Size())
	buffer := make([]byte, size)
	(*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Data =
		uintptr((*MValue)(unsafe.Pointer(&value)).Ptr)
	return buffer
}
