package wal

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type WriteCommand struct {
	RecordType    io.EnumRecordType
	WALKeyPath    string
	VarRecLen     int
	Offset, Index int64
	Data          []byte
	// DataShapes with Epoch column
	DataShapes    []io.DataShape
}

// Convert WriteCommand to string for debuging/presentation
func (wc *WriteCommand) toString() string {
	return fmt.Sprintf("WC[%v] WALKeyPath:%s (len:%d, off:%d, idx:%d, dsize:%d)", wc.RecordType, wc.WALKeyPath, wc.VarRecLen, wc.Offset, wc.Index, len(wc.Data))
}
