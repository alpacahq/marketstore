package executor

/*
#include "rewriteBuffer.h"
#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument -std=c99
*/
import "C"
import (
	"encoding/binary"
	"github.com/alpacahq/marketstore/utils/io"
	_ "github.com/alpacahq/marketstore/utils/log"
	_ "go.uber.org/zap"
	"math"
	"unsafe"
)

func RewriteBuffer_C(buffer []byte, varRecLen, numVarRecords, intervalsPerDay uint32, intervalStartEpoch uint64) []byte {
	//// Variable records use the raw element sizes plus a 4-byte trailer for interval ticks
	//f.variableRecordLength = int32(f.getFieldRecordLength()) + 4 // Variable records have a 4-byte trailer
	// temporary result buffer
	rbTemp := make([]byte, numVarRecords*(varRecLen+8)) // Add the extra space for epoch

	arg1 := (*C.char)(unsafe.Pointer(&buffer[0]))
	arg4 := (*C.char)(unsafe.Pointer(&rbTemp[0]))

	C.rewriteBuffer(arg1, C.int(varRecLen), C.int(numVarRecords), arg4,
		C.int64_t(intervalsPerDay), C.int64_t(intervalStartEpoch))

	return rbTemp
}

// RewriteBuffer converts variable_length records to the result buffer.
//
// variable records in a file: [Actual Data (VarRecLen-4 byte) , Interval Ticks(4 byte) ]
// rewriteBuffer converts the binary data to [EpochSecond(8 byte), Actual Data(VarRecLen-4 byte), Nanoseconds(4 byte) ] format.
//
// buffer
// +-----------------------VarRecLen [byte]---+-----------------------+
// +   Actual Data(Ask,Bid, etc.)             | IntevalTicks(4byte)    |
// +------------------------------------------+------------------------+
//
// ↓ rewriteBuffer
//
// rbTemp (= temporary result buffer)
// +--------------------+--VarRecLen + 8 [byte]-----+-------------------+
// + EpochSecond(8byte) | Actual Data(Ask,Bid, etc) | Nanosecond(4byte) |
// +--------------------+----------------------------+------------------+
func RewriteBuffer_Go(buffer []byte, varRecLen, numVarRecords uint32, intervalsPerDay uint32, intervalStartEpoch uint64) []byte {
	// temporary result buffer
	rbTemp := make([]byte, numVarRecords*(varRecLen+8)) // Add the extra space for epoch

	var j, ii, cursor uint32
	b := make([]byte, 8)
	n := make([]byte, 4)
	for j = 0; j < numVarRecords; j++ {

		intervalTicks := buffer[(j+1)*varRecLen-4 : (j+1)*varRecLen]
		it := io.ToUInt32(intervalTicks)

		// Expand ticks (32-bit) into epoch and nanos
		second, nanosecond := GetTimeFromTicks(intervalStartEpoch, intervalsPerDay, it)
		binary.LittleEndian.PutUint64(b, second)

		// copy Epoch second to the result buffer
		cursor = j * (varRecLen + 8)
		for ii = 0; ii < 8; ii++ {
			rbTemp[cursor+ii] = b[ii]
		}

		// copy actual data (e.g. Ask, Bid) to the result buffer after the Epoch Second
		for ii = 0; ii < varRecLen-4; ii++ {
			rbTemp[cursor+8+ii] = buffer[(j*varRecLen)+ii]
		}

		// copy nanosecond to the result buffer after the Epoch Second
		binary.LittleEndian.PutUint32(n, nanosecond)
		for ii = 0; ii < 4; ii++ {
			rbTemp[cursor+varRecLen+4+ii] = n[ii]
		}
	}

	return rbTemp
}

// GetTimeFromTicks Takes two time components, the start of the interval and the number of
// interval ticks to the timestamp and returns an epoch time (seconds) and
// the number of nanoseconds of fractional time within the last second as a remainder
func GetTimeFromTicks(intervalStart uint64, intervalsPerDay, intervalTicks uint32) (sec uint64, nanosec uint32) {
	// intervalsPerDay = 86400(sec) / timeframe(sec)
	// nanosecPerIntervalTick = timeframe(sec) *10^9 / 2^32
	//                        = (86400 / intervalsPerDay) * 10^9 / 2^32
	// nanosec = intervalTicks * nanosecPerIntervalTick
	//         = intervalTicks * 86400 * 10^9 / (2^32 * intervalsPerDay)
	//         = intervalTicks * 20116.5676117 / intervalsPerDay

	nanoseconds := uint64(math.Round(float64(intervalTicks) * 20116.5676117 / float64(intervalsPerDay)))

	// if nanoseconds value is larger than 1 second
	if nanoseconds >= 1000000000 {
		plusSec := nanoseconds / 1000000000
		nanoseconds -= plusSec * 1000000000
		return intervalStart + plusSec, uint32(nanoseconds)
	}
	return intervalStart, uint32(nanoseconds)
}
