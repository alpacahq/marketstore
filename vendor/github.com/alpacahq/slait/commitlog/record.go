package commitlog

const (
	nanosecPos      = 0
	sizePos         = 8
	recordHeaderLen = 12
)

type Record []byte

func NewRecord(nanosec int64, payload []byte) Record {
	rec := make([]byte, recordHeaderLen, recordHeaderLen+len(payload))
	Encoding.PutUint64(rec[nanosecPos:nanosecPos+8], uint64(nanosec))
	size := int32(len(payload))
	Encoding.PutUint32(rec[sizePos:sizePos+4], uint32(size))
	rec = append(rec, payload...)
	return rec
}
