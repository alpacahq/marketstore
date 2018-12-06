package executor

import (
	"github.com/alpacahq/marketstore/utils"
	"github.com/klauspost/compress/snappy"
	"os"
	"unsafe"

	. "github.com/alpacahq/marketstore/utils/io"
)

/*
#include "rewriteBuffer.h"
#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument
*/
import "C"

func (r *reader) readSecondStage(bufMeta []bufferMeta, limitCount int32, direction DirectionEnum) (rb []byte, err error) {
	/*
		Here we use the bufFileMap which has index data for each file, then we read
		the target data into the resultBuffer up to the limitCount number of records
	*/
	var varRecLen int
	for _, md := range bufMeta {
		varRecLen = md.VarRecLen
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
		var totalDatalen int
		// Without compression we have the exact size of the output buffer
		numIndexRecords := len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
		numberLeftToRead := int(limitCount)
		if utils.InstanceConfig.DisableVariableCompression {
			for i := 0; i < numIndexRecords; i++ {
				datalen := int(ToInt64(indexBuffer[i*24+16:]))
				numVarRecords := datalen / varRecLen // TODO: This doesn't work with compression
				if direction == FIRST {
					if numVarRecords >= numberLeftToRead {
						numVarRecords = numberLeftToRead
					}
				}
				totalDatalen += numVarRecords * (varRecLen + 8)
				numberLeftToRead -= numVarRecords
			}
		} else {
			// With compression, the size is approximate, multiply by estimated ratio to get close
			for i := 0; i < numIndexRecords; i++ {
				totalDatalen += int(ToInt64(indexBuffer[i*24+16:]))
			}
			totalDatalen *= 4
		}

		numIndexRecords = len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
		numberLeftToRead = int(limitCount)
		//rb = make([]byte, 0)
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

			if !utils.InstanceConfig.DisableVariableCompression {
				buffer, err = snappy.Decode(nil, buffer)
				if err != nil {
					return nil, err
				}
			}

			// Loop over the variable records and prepend the index time to each
			numVarRecords := len(buffer) / varRecLen
			if direction == FIRST {
				if numVarRecords >= numberLeftToRead {
					numVarRecords = numberLeftToRead
				}
			}
			rbTemp := make([]byte, numVarRecords*(varRecLen+8)) // Add the extra space for epoch

			arg1 := (*C.char)(unsafe.Pointer(&buffer[0]))
			arg4 := (*C.char)(unsafe.Pointer(&rbTemp[0]))
			C.rewriteBuffer(arg1, C.int(varRecLen), C.int(numVarRecords), arg4,
				C.int64_t(md.Intervals), C.int64_t(intervalStartEpoch))

			//rb = append(rb, rbTemp...)
			if (rbCursor + len(rbTemp)) > totalDatalen {
				totalDatalen += totalDatalen
				rb2 := make([]byte, totalDatalen)
				copy(rb2[:rbCursor], rb[:rbCursor])
				rb = rb2
			}
			copy(rb[rbCursor:], rbTemp)
			rbCursor += len(rbTemp)

			numberLeftToRead -= numVarRecords
			if direction == FIRST {
				if numberLeftToRead == 0 {
					break
				}
			}
		}
		rb = rb[:rbCursor]
		fp.Close()
	}
	if direction == LAST {
		// Chop the last N records out of the results
		numVarRecords := len(rb) / (varRecLen + 8)
		if int(limitCount) < numVarRecords {
			offset := (varRecLen + 8) * (numVarRecords - int(limitCount))
			rb = rb[offset:]
		}
	}
	return rb, nil
}
