package executor

import (
	"reflect"
	"unsafe"
)

//#include "stdlib.h"
//#include "string.h"
//#include "timsort.h"
//#include "stdio.h"
//#include "stdint.h"
//#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument
//#define TYPE uint32_t
//static int compare(const void *a, const void *b)
//{
//  const TYPE da = *((const TYPE *) a);
//  const TYPE db = *((const TYPE *) b);
//  return (da < db) ? -1 : (da == db) ? 0 : 1;
//}
//void tsort(void *base, size_t nel, size_t width) {
//	timsort(base, nel, width, compare);
//}
//void tsortbuf(char *buf, size_t nel, size_t rec_nel) {
//	int i = 0;
//
//	size_t num_words = nel/rec_nel;
//	char* words = (char *)malloc(nel);
//
//  for (i = 0; i < num_words; i++) {
//		memcpy(words+i*rec_nel, &buf[(i+1)*rec_nel-4], 4);
//		memcpy(words+i*rec_nel+4, &buf[i*rec_nel], rec_nel-4);
//	}
//
//  tsort(words, num_words, rec_nel);
//
//  for (i = 0; i < num_words; i++) {
//		memcpy(buf+i*rec_nel, &words[i*rec_nel+4], rec_nel-4);
//		memcpy(buf+((i+1)*rec_nel)-4, &words[i*rec_nel], 4);
//  }
//}
import "C"

func TimSortUINT32(arr interface{}) {
	refVal := reflect.ValueOf(arr)

	C.tsort(
		unsafe.Pointer(refVal.Pointer()),
		C.size_t(refVal.Len()),
		C.size_t(reflect.TypeOf(arr).Elem().Size()),
	)
}

func TimSortBufferUINT32(buf unsafe.Pointer, dataLen, recLen uint64) {
	C.tsortbuf(
		(*C.char)(buf),
		C.size_t(dataLen),
		C.size_t(recLen),
	)
}
