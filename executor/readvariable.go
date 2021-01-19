package executor

import (
	"os"

	"github.com/klauspost/compress/snappy"

	. "github.com/alpacahq/marketstore/v4/utils/io"
)

func (r *Reader) readSecondStage(bufMeta []bufferMeta) (rb []byte, err error) {
	/*
		Here we use the bufFileMap which has index data for each file, then we read
		the target data into the resultBuffer up to the limitCount number of records
	*/
	var varRecLen int
	// resultBuffers for all bufMetas
	totalBuf := make([]byte, 0)
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
		if r.disableVariableCompression {
			for i := 0; i < numIndexRecords; i++ {
				datalen := int(ToInt64(indexBuffer[i*24+16:]))
				numVarRecords := datalen / varRecLen // TODO: This doesn't work with compression
				totalDatalen += numVarRecords * (varRecLen + 8)
			}
		} else {
			// With compression, the size is approximate, multiply by estimated ratio to get close
			for i := 0; i < numIndexRecords; i++ {
				totalDatalen += int(ToInt64(indexBuffer[i*24+16:]))
			}
			totalDatalen *= 4
		}

		numIndexRecords = len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
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

			if !r.disableVariableCompression {
				buffer, err = snappy.Decode(nil, buffer)
				if err != nil {
					return nil, err
				}
			}

			// Loop over the variable records and prepend the index time to each
			numVarRecords := len(buffer) / varRecLen
			rbTemp := RewriteBuffer(buffer,
				uint32(varRecLen), uint32(numVarRecords), uint32(md.Intervals), uint64(intervalStartEpoch))

			//rb = append(rb, rbTemp...)
			if (rbCursor + len(rbTemp)) > totalDatalen {
				totalDatalen += totalDatalen
				rb2 := make([]byte, totalDatalen)
				copy(rb2[:rbCursor], rb[:rbCursor])
				rb = rb2
			}
			copy(rb[rbCursor:], rbTemp)
			rbCursor += len(rbTemp)

		}
		rb = rb[:rbCursor]
		fp.Close()

		totalBuf = append(totalBuf, rb...)
	}
	return totalBuf, nil
}
