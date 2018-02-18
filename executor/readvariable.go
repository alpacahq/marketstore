package executor

import (
	"os"
	"unsafe"

	. "github.com/alpacahq/marketstore/utils/io"
)

/*
#include "rewriteBuffer.h"
#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument
*/
import "C"

func (r *reader) readSecondStage(bufMeta []bufferMeta) (rb []byte, err error) {
	/*
		Here we use the bufFileMap which has index data for each file, then we read
		the target data into the resultBuffer
	*/
	for _, md := range bufMeta {
		file := md.FullPath
		indexBuffer := md.Data

		// Open the file to read the data
		fp, err := os.OpenFile(file, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		/*
			Calculate how much space is needed in the results buffer
		*/
		numIndexRecords := len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
		var totalDatalen int
		for i := 0; i < numIndexRecords; i++ {
			datalen := int(ToInt64(indexBuffer[i*24+16:]))
			numVarRecords := datalen / md.VarRecLen
			totalDatalen += numVarRecords * (md.VarRecLen + 8)
		}
		rb = make([]byte, totalDatalen)
		var rbCursor int
		for i := 0; i < numIndexRecords; i++ {
			intervalStartEpoch := ToInt64(indexBuffer[i*24:])
			offset := ToInt64(indexBuffer[i*24+8:])
			datalen := ToInt64(indexBuffer[i*24+16:])
			//			fmt.Println("indxlen, off, len", len(indexBuffer), offset, datalen)

			buffer := make([]byte, datalen)
			_, err = fp.ReadAt(buffer, offset)
			if err != nil {
				return nil, err
			}

			// Loop over the variable records and prepend the index time to each
			numVarRecords := len(buffer) / md.VarRecLen
			rbTemp := make([]byte, numVarRecords*(md.VarRecLen+8)) // Add the extra space for epoch

			arg1 := (*C.char)(unsafe.Pointer(&buffer[0]))
			arg4 := (*C.char)(unsafe.Pointer(&rbTemp[0]))
			C.rewriteBuffer(arg1, C.int(md.VarRecLen), C.int(numVarRecords), arg4,
				C.int64_t(md.Intervals), C.int64_t(intervalStartEpoch))

			//rb = append(rb, rbTemp...)
			copy(rb[rbCursor:], rbTemp)
			rbCursor += len(rbTemp)
		}
		fp.Close()
	}
	return rb, nil
}
