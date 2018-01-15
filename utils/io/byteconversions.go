package io

import (
	"reflect"
	"unsafe"
)

func CopySliceByte(ib interface{}, is interface{}) interface{} {
	buffer := ib.([]byte)

	structValue := reflect.ValueOf(is)
	structType := structValue.Type()
	structSize := structType.Size()
	structSliceType := reflect.SliceOf(structType)

	Len := len(buffer) / int(structSize)
	Cap := Len
	structSlice := reflect.MakeSlice(structSliceType, Len, Cap)

	p_bufferData := unsafe.Pointer(&buffer[0])
	p_structData := unsafe.Pointer(structSlice.Pointer())
	for i := 0; i < len(buffer); i++ {
		sd := (*byte)(unsafe.Pointer(uintptr(p_structData) + uintptr(i)))
		bd := (*byte)(unsafe.Pointer(uintptr(p_bufferData) + uintptr(i)))
		*sd = *bd
	}

	return structSlice.Interface()
}

// This is a *copy* of the "Value" struct inside the reflect package
type MValue struct {
	typ uintptr
	Ptr unsafe.Pointer
}

func SwapSliceByte(src_byte_slice interface{}, target_type interface{}) interface{} {
	buffer := src_byte_slice.([]byte)

	structValue := reflect.ValueOf(target_type)
	structType := structValue.Type()
	structSize := structType.Size()
	// structSize := binary.Size(target_type)
	structSliceType := reflect.SliceOf(structType)

	Len := len(buffer) / int(structSize)
	Cap := Len
	structSlice := reflect.MakeSlice(structSliceType, Len, Cap)

	// Set the new slice header data to that of the byte slice
	(*reflect.SliceHeader)(unsafe.Pointer((*(*MValue)(unsafe.Pointer(&structSlice))).Ptr)).Data =
		(*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Data

	return structSlice.Interface()
}

func ToUint8(b []byte) uint8 {
	return *(*uint8)(unsafe.Pointer(&b[0]))
}
func ToInt8(b []byte) int8 {
	return *(*int8)(unsafe.Pointer(&b[0]))
}
func ToInt16(b []byte) int16 {
	return *(*int16)(unsafe.Pointer(&b[0]))
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
func ToFloat32(b []byte) float32 {
	return *(*float32)(unsafe.Pointer(&b[0]))
}
func ToFloat64(b []byte) float64 {
	return *(*float64)(unsafe.Pointer(&b[0]))
}

func SwapSliceData(src_slice interface{}, target_type interface{}) interface{} {
	leftValue := reflect.ValueOf(src_slice)
	leftLen := leftValue.Len()
	leftType := leftValue.Type().Elem()
	leftSize := leftType.Size()
	//	fmt.Printf("LeftType: %s LeftLen: %d LeftSize: %d \n",leftType, leftLen, leftSize)

	rightValue := reflect.ValueOf(target_type)
	rightType := rightValue.Type()
	rightSize := rightType.Size()
	//rightSize := binary.Size(target_type)
	rightSliceType := reflect.SliceOf(rightType)

	//	fmt.Printf("LeftType: %s LeftLen: %d LeftSize: %d RightSize: %d\n",leftType, leftLen, leftSize, rightSize)
	LenRight := (leftLen * int(leftSize)) / int(rightSize)
	CapRight := LenRight
	rightSlice := reflect.MakeSlice(rightSliceType, LenRight, CapRight)

	// Set the data pointer of the right slice equal to that of the left
	(*reflect.SliceHeader)(unsafe.Pointer((*(*MValue)(unsafe.Pointer(&rightSlice))).Ptr)).Data =
		(*reflect.SliceHeader)(unsafe.Pointer((*(*MValue)(unsafe.Pointer(&leftValue))).Ptr)).Data
	return rightSlice.Interface()
}

// Cast sliceData's memory chunk to a byte slice without copy.
func CastToByteSlice(sliceData interface{}) []byte {
	sliceValue := reflect.ValueOf(sliceData)
	sliceLen := sliceValue.Len()
	elemType := sliceValue.Type().Elem()
	elemSize := elemType.Size()

	bufLen := sliceLen * int(elemSize)
	buffer := make([]byte, 0, 0)
	bufHeader := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	bufHeader.Len = bufLen
	bufHeader.Cap = bufLen
	bufHeader.Data = sliceValue.Pointer()

	return buffer
}

// Takes a primary (non slice, non pointer) type and returns a []byte of the base type data
func DataToByteSlice(srcData interface{}) []byte {
	value := reflect.ValueOf(srcData)
	size := int(value.Type().Size())
	buffer := make([]byte, size, size)
	(*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Data =
		uintptr(unsafe.Pointer((*(*MValue)(unsafe.Pointer(&value))).Ptr))
	return buffer
}
