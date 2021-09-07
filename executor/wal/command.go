package wal

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// WriteCommand is a write request for WriteAheadLog (WAL).
// One WriteCommand can have multiple row records that have the same index, in case of VariableLength record type.
type WriteCommand struct {
	RecordType io.EnumRecordType
	// WALKeyPath is the relative path from the root directory.
	// e.g. "WALFile.1621901771897875000.walfile"
	WALKeyPath string
	// VarRecLen is used only in case of VARIABLE recordType.
	// (The sum of field lengths in elementTypes without Epoch column) + 4 bytes(for intervalTicks)
	VarRecLen     int
	Offset, Index int64
	// Data can be for multiple row records.
	// If the record type is FIXED, one row data is the series of columns without Epoch column.
	// If it's VARIABLE, one row data is the series of columns without Epoch Column, but with IntervalTick(4byte).
	Data []byte
	// DataShapes with Epoch column
	DataShapes []io.DataShape
}

// Convert WriteCommand to string for debuging/presentation
func (wc *WriteCommand) String() string {
	return fmt.Sprintf("WC[%v] WALKeyPath:%s (len:%d, off:%d, idx:%d, dsize:%d)", wc.RecordType, wc.WALKeyPath, wc.VarRecLen, wc.Offset, wc.Index, len(wc.Data))
}

//func WriteCommandsToProto(commands []*WriteCommand) []*proto.WriteCommand {
//	ret := make([]*proto.WriteCommand, len(commands))
//	for i, wc := range commands {
//		ret[i] = &proto.WriteCommand{
//			RecordType:           proto.RecordType(wc.RecordType),
//			WalKeyPath:           wc.WALKeyPath,
//			VariableRecordLength: wc.VarRecLen,
//			Offset:               wc.Offset,
//			Index:                wc.Index,
//			Data:                 wc.Data,
//		}
//	}
//	return ret
//}
//
//func WriteCommandsFromProto(commands []*proto.WriteCommand) []*WriteCommand {
//	ret := make([]*WriteCommand, len(commands))
//	for i, c := range commands {
//		ret[i] = &WriteCommand{
//			RecordType: io.EnumRecordType(c.RecordType),
//			WALKeyPath: c.WalKeyPath,
//			VarRecLen:  c.VariableRecordLength,
//			Offset:     c.Offset,
//			Index:      c.Index,
//			Data:       c.Data,
//		}
//	}
//	return ret
//}
