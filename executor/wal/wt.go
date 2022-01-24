package wal

import "github.com/alpacahq/marketstore/v4/utils/io"

type WTSet struct {
	// Direct or Indirect IO (for variable or fixed length records)
	RecordType io.EnumRecordType
	// FilePath is an absolute path of the WAL file. The string is ASCII encoded without a trailing null
	FilePath string
	// Length of each data element in this set in bytes, excluding the index
	// In case of VARIABLE recordType, this value is always 24.
	// (=Index(8byte), Offset(8byte), DataLen of the the variable length records(8byte))
	DataLen int
	// Used only in case of VARIABLE recordType.
	// (The sum of field lengths in elementTypes without Epoch column) + 4 bytes(for intervalTicks)
	VarRecLen int
	// Data bytes
	Buffer OffsetIndexBuffer
	// Data Shape with Epoch Column
	DataShapes []io.DataShape
}

func NewWTSet(recordType io.EnumRecordType, filePath string, dataLen, varRecLen int,
	data OffsetIndexBuffer, dataShapes []io.DataShape,
) WTSet {
	return WTSet{
		RecordType: recordType,
		FilePath:   filePath,
		DataLen:    dataLen,
		VarRecLen:  varRecLen,
		Buffer:     data,
		DataShapes: dataShapes,
	}
}
