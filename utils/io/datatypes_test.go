package io

import (
	"encoding/binary"
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var TestData = []byte{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
}
var (
	RecordLength = 10
	NumRecords   = 3
	Offset       = 2
)

func Test_getFloat32Column(t *testing.T) {
	// --- when ---
	col := getFloat32Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, NumRecords)
	b1 := make([]byte, 4)
	b2 := make([]byte, 4)
	b3 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b1, math.Float32bits(col[0]))
	binary.LittleEndian.PutUint32(b2, math.Float32bits(col[1]))
	binary.LittleEndian.PutUint32(b3, math.Float32bits(col[2]))

	assert.Equal(t, b1, []byte{2, 3, 4, 5})
	assert.Equal(t, b2, []byte{12, 13, 14, 15})
	assert.Equal(t, b3, []byte{22, 23, 24, 25})
}

func Test_getFloat32Column_0record(t *testing.T) {
	// --- when ---
	col := getFloat32Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []float32{})
}

func Test_getFloat64Column(t *testing.T) {
	// --- when ---
	col := getFloat64Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 8)
	b2 := make([]byte, 8)
	b3 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b1, math.Float64bits(col[0]))
	binary.LittleEndian.PutUint64(b2, math.Float64bits(col[1]))
	binary.LittleEndian.PutUint64(b3, math.Float64bits(col[2]))

	assert.Equal(t, b1, []byte{2, 3, 4, 5, 6, 7, 8, 9})
	assert.Equal(t, b2, []byte{12, 13, 14, 15, 16, 17, 18, 19})
	assert.Equal(t, b3, []byte{22, 23, 24, 25, 26, 27, 28, 29})
}

func Test_getFloat64Column_0record(t *testing.T) {
	// --- when ---
	col := getFloat64Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []float64{})
}

func Test_getInt16Column(t *testing.T) {
	// --- when ---
	col := getInt16Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 2)
	b2 := make([]byte, 2)
	b3 := make([]byte, 2)
	binary.LittleEndian.PutUint16(b1, *(*uint16)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint16(b2, *(*uint16)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint16(b3, *(*uint16)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3})
	assert.Equal(t, b2, []byte{12, 13})
	assert.Equal(t, b3, []byte{22, 23})
}

func Test_getInt16Column_0record(t *testing.T) {
	// --- when ---
	col := getInt16Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []int16{})
}

func Test_getInt32Column(t *testing.T) {
	// --- when ---
	col := getInt32Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 4)
	b2 := make([]byte, 4)
	b3 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b1, *(*uint32)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint32(b2, *(*uint32)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint32(b3, *(*uint32)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3, 4, 5})
	assert.Equal(t, b2, []byte{12, 13, 14, 15})
	assert.Equal(t, b3, []byte{22, 23, 24, 25})
}

func Test_getInt32Column_0record(t *testing.T) {
	// --- when ---
	col := getInt32Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []int32{})
}

func Test_getInt64Column(t *testing.T) {
	// --- when ---
	col := getInt64Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 8)
	b2 := make([]byte, 8)
	b3 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b1, *(*uint64)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint64(b2, *(*uint64)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint64(b3, *(*uint64)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3, 4, 5, 6, 7, 8, 9})
	assert.Equal(t, b2, []byte{12, 13, 14, 15, 16, 17, 18, 19})
	assert.Equal(t, b3, []byte{22, 23, 24, 25, 26, 27, 28, 29})
}

func Test_getInt64Column_0record(t *testing.T) {
	// --- when ---
	col := getInt64Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []int64{})
}

func Test_getUInt8Column(t *testing.T) {
	// --- when ---
	col := getUInt8Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := *(*uint8)(unsafe.Pointer(&col[0]))
	b2 := *(*uint8)(unsafe.Pointer(&col[1]))
	b3 := *(*uint8)(unsafe.Pointer(&col[2]))

	assert.Equal(t, b1, uint8(2))
	assert.Equal(t, b2, uint8(12))
	assert.Equal(t, b3, uint8(22))
}

func Test_getUInt8Column_0record(t *testing.T) {
	// --- when ---
	col := getUInt8Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []uint8{})
}

func Test_getUInt16Column(t *testing.T) {
	// --- when ---
	col := getUInt16Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 2)
	b2 := make([]byte, 2)
	b3 := make([]byte, 2)
	binary.LittleEndian.PutUint16(b1, *(*uint16)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint16(b2, *(*uint16)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint16(b3, *(*uint16)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3})
	assert.Equal(t, b2, []byte{12, 13})
	assert.Equal(t, b3, []byte{22, 23})
}

func Test_getUInt16Column_0record(t *testing.T) {
	// --- when ---
	col := getUInt16Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []uint16{})
}

func Test_getUInt32Column(t *testing.T) {
	// --- when ---
	col := getUInt32Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 4)
	b2 := make([]byte, 4)
	b3 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b1, *(*uint32)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint32(b2, *(*uint32)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint32(b3, *(*uint32)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3, 4, 5})
	assert.Equal(t, b2, []byte{12, 13, 14, 15})
	assert.Equal(t, b3, []byte{22, 23, 24, 25})
}

func Test_getUInt32Column_0record(t *testing.T) {
	// --- when ---
	col := getUInt32Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []uint32{})
}

func Test_getUInt64Column(t *testing.T) {
	// --- when ---
	col := getUInt64Column(Offset, RecordLength, NumRecords, TestData)

	// --- then ---
	assert.Len(t, col, 3)
	b1 := make([]byte, 8)
	b2 := make([]byte, 8)
	b3 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b1, *(*uint64)(unsafe.Pointer(&col[0])))
	binary.LittleEndian.PutUint64(b2, *(*uint64)(unsafe.Pointer(&col[1])))
	binary.LittleEndian.PutUint64(b3, *(*uint64)(unsafe.Pointer(&col[2])))

	assert.Equal(t, b1, []byte{2, 3, 4, 5, 6, 7, 8, 9})
	assert.Equal(t, b2, []byte{12, 13, 14, 15, 16, 17, 18, 19})
	assert.Equal(t, b3, []byte{22, 23, 24, 25, 26, 27, 28, 29})
}

func Test_getUInt64Column_0record(t *testing.T) {
	// --- when ---
	col := getUInt64Column(Offset, RecordLength, 0, TestData)
	// --- then ---
	assert.Equal(t, col, []uint64{})
}
