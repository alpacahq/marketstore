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

const (
	RecordLength = 24 // number of bytes in record. Three fields, {epoch, offset, len}, 8 bytes each
)

func (r *reader) readSecondStage(bufMetas []bufferMeta, limitCount int32, direction DirectionEnum) (rb []byte, err error) {
	/*
		Here we use the bufFileMap which has index data for each file, then we read
		the target data into the resultBuffer up to the limitCount number of records
	*/

	// variable record length
	var varRecLen int
	for _, bufMeta := range bufMetas {
		err = r.readBufMeta(bufMeta, limitCount, direction)
		if err != nil {
			return nil, err
		}
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

// getLengthOfRecord returns the length of the variable-length data that i-th index record points
func getLengthOfRecord(i int, indexBuffer []byte) int {
	// an index record = epoch(8byte) + offset(8byte) + length(8byte)
	return int(ToInt64(indexBuffer[i*RecordLength+16:]))
}

func getOffsetOfRecord(i int, indexBuffer []byte) int64 {
	return ToInt64(indexBuffer[i*RecordLength+8:])
}

func getEpochOfRecord(i int, indexBuffer []byte) int64 {
	return ToInt64(indexBuffer[i*RecordLength:])
}

func getTotalDataLen(indexBuffer []byte, varRecLen int, numberLeftToRead *int, numIndexRecords int, direction DirectionEnum) int {
	var totalDatalen int

	if utils.InstanceConfig.DisableVariableCompression {
		// Without compression
		for i := 0; i < numIndexRecords; i++ {
			datalen := getLengthOfRecord(i, indexBuffer)

			// the number of variable-length records in this index(=in this epoch)
			numVarRecords := datalen / varRecLen // TODO: This doesn't work with compression
			if direction == FIRST {
				if numVarRecords >= *numberLeftToRead {
					numVarRecords = *numberLeftToRead
				}
			}
			totalDatalen += numVarRecords * (varRecLen + 8)
			*numberLeftToRead -= numVarRecords
		}
	} else {
		// With compression, the size is approximate, multiply by estimated ratio to get close
		for i := 0; i < numIndexRecords; i++ {
			totalDatalen += getLengthOfRecord(i, indexBuffer)
		}
		totalDatalen *= 4 // estimated compression rate = 4
	}

	return totalDatalen
}

func (r *reader) readBufMeta(bufMeta bufferMeta, limitCount int32, direction DirectionEnum) error{
		// the number of bytes in a variable-length record
		varRecLen := bufMeta.VarRecLen
		file := bufMeta.FullPath
		indexBuffer := bufMeta.Data

		// Open the file to read the data
		fp, err := os.OpenFile(file, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		defer fp.Close()

		/*
			Calculate how much space is needed in the results buffer
		*/
		// the number of index records in this file
		// Without compression we have the exact size of the output buffer
		numIndexRecords := len(indexBuffer) / RecordLength
		numberLeftToRead := int(limitCount)
		totalDatalen := getTotalDataLen(indexBuffer, varRecLen, &numberLeftToRead, numIndexRecords, direction)

		//rb = make([]byte, 0)
		rb := make([]byte, totalDatalen)
		var rbCursor int
		for i := 0; i < numIndexRecords; i++ {
			intervalStartEpoch := getEpochOfRecord(i, indexBuffer)
			offset := getOffsetOfRecord(i, indexBuffer)
			datalen := getLengthOfRecord(i, indexBuffer)
			//			fmt.Println("indxlen, off, len", len(indexBuffer), offset, datalen)

			buffer := make([]byte, datalen)
			_, err = fp.ReadAt(buffer, offset)
			if err != nil {
				return err
			}

			if !utils.InstanceConfig.DisableVariableCompression {
				buffer, err = snappy.Decode(nil, buffer)
				if err != nil {
					return err
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
				C.int64_t(bufMeta.Intervals), C.int64_t(intervalStartEpoch))

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
	}

}
