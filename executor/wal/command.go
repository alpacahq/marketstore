package wal

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// WriteCommand is a write request for WriteAheadLog (WAL).
// One WriteCommand can have multiple row records that have the same index, in case of VariableLength record type.
type WriteCommand struct {
	RecordType    io.EnumRecordType
	WALKeyPath    string
	VarRecLen     int
	Offset, Index int64
	// Data can be for multiple row records.
	Data []byte
	// DataShapes with Epoch column
	DataShapes []io.DataShape
}

// Convert WriteCommand to string for debuging/presentation
func (wc *WriteCommand) toString() string {
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
