package executor

import (
	"reflect"
	"unsafe"
)

//#include "quickSort.h"
//#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument
import "C"

func QuickSortKeyAtEndUINT32(ai interface{}) {
	recSize := reflect.TypeOf(ai).Elem().Size()
	refValue := reflect.ValueOf(ai)
	lenSlice := int64(refValue.Len())
	if lenSlice == 0 {
		return
	}
	lenData := int64(recSize) * lenSlice

	arg1 := (*C.char)(unsafe.Pointer(refValue.Pointer()))
	C.quickSortKeyAtEndUINT32(arg1, C.int64_t(lenData), C.int64_t(recSize))
}
