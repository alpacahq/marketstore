package wal

import "github.com/alpacahq/marketstore/v4/utils/io"

type OffsetIndexBuffer []byte

// Offset is the byte offset from the head of the file to write the record.
// offset = (index-1)*int64(recordSize) + FileHeadersSize.
// used to seek the point to write the record to the file.
func (b OffsetIndexBuffer) Offset() int64 {
	return io.ToInt64(b[:8])
}

// Index indicates the number of timeframes before this data from Jan 1st, 00:00:00 of the year.
// Note that Index starts from 1 (unless the timeframe is 1D).
// e.g. if timeframe=1Min and the time of the record = Jan 2nd, 03:04:05,
// then its index is 1625 because it's 1day 3hour 4min (=1624min) from Jan 1st, 00:00:00.
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
