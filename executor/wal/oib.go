package wal

import "github.com/alpacahq/marketstore/v4/utils/io"

type OffsetIndexBuffer []byte

func (b OffsetIndexBuffer) Offset() int64 {
	return io.ToInt64(b[:8])
}

func (b OffsetIndexBuffer) Index() int64 {
	return io.ToInt64(b[8:16])
}

func (b OffsetIndexBuffer) IndexAndPayload() []byte {
	return b[8:]
}

// Payload can be multiple rows data that have the same index, in case of VariableLength record type.
func (b OffsetIndexBuffer) Payload() []byte {
	return b[16:]
}
