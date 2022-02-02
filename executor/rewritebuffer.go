package executor

import (
	"encoding/binary"
	"math"

	_ "go.uber.org/zap"

	"github.com/alpacahq/marketstore/v4/utils/io"
	_ "github.com/alpacahq/marketstore/v4/utils/log"
)

// RewriteBuffer converts variable_length records to the result buffer.
//
// variable records in a file: [Actual Data (VarRecLen-4 byte) , Interval Ticks(4 byte) ]
// rewriteBuffer converts the binary data to
// [EpochSecond(8 byte), Actual Data(VarRecLen-4 byte), Nanoseconds(4 byte) ]
// format.
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
// +--------------------+----------------------------+------------------+.
func RewriteBuffer(buffer []byte, varRecLen, numVarRecords, intervalsPerDay uint32, intervalStartEpoch uint64) []byte {
	// temporary result buffer
	rbTemp := make([]byte, numVarRecords*(varRecLen+epochLenBytes)) // Add the extra space for epoch

	var j, ii, cursor uint32
	b := make([]byte, epochLenBytes)
	n := make([]byte, nanosecLenBytes)
	for j = 0; j < numVarRecords; j++ {
		intervalTicks := buffer[(j+1)*varRecLen-intervalTicksLenBytes : (j+1)*varRecLen]
		it := io.ToUInt32(intervalTicks)

		// Expand ticks (32-bit) into epoch and nanos
		second, nanosecond := GetTimeFromTicks(intervalStartEpoch, intervalsPerDay, it)
		binary.LittleEndian.PutUint64(b, second)

		// copy Epoch second to the result buffer
		cursor = j * (varRecLen + epochLenBytes)
		for ii = 0; ii < epochLenBytes; ii++ {
			rbTemp[cursor+ii] = b[ii]
		}

		// copy actual data (e.g. Ask, Bid) to the result buffer after the Epoch Second
		for ii = 0; ii < varRecLen-intervalTicksLenBytes; ii++ {
			rbTemp[cursor+epochLenBytes+ii] = buffer[(j*varRecLen)+ii]
		}

		// copy nanosecond to the result buffer after the Epoch Second
		binary.LittleEndian.PutUint32(n, nanosecond)
		for ii = 0; ii < nanosecLenBytes; ii++ {
			rbTemp[cursor+epochLenBytes+varRecLen-intervalTicksLenBytes+ii] = n[ii]
		}
	}

	return rbTemp
}

// GetTimeFromTicks Takes two time components, the start of the interval and the number of
// interval ticks to the timestamp and returns an epoch time (seconds) and
// the number of nanoseconds of fractional time within the last second as a remainder.
func GetTimeFromTicks(intervalStart uint64, intervalsPerDay, intervalTicks uint32) (sec uint64, nanosec uint32) {
	const (
		ticksPerIntervalDivSecsPerDay float64 = 49710.269629629629629629629629629
		nanosecond                    float64 = 1000000000
		subnanosecond                 float64 = 100000000
	)

	fractionalSeconds := float64(intervalTicks) / (float64(intervalsPerDay) * ticksPerIntervalDivSecsPerDay)
	subseconds := nanosecond * (fractionalSeconds - math.Floor(fractionalSeconds))
	if subseconds >= nanosecond {
		subseconds -= nanosecond
		fractionalSeconds += 1
	}

	// in order to keep compatibility with the old rewriteBuffer implemented in C with some round error,
	// fractionalSeconds should be rounded here.
	sec = intervalStart + uint64(math.Round(fractionalSeconds*subnanosecond)/subnanosecond)
	// round the subseconds after the decimal point to minimize the cancellation error of subseconds
	// round( subseconds ) = (int32_t)(subseconds + 0.5)
	const round = 0.5
	nanosec = uint32(subseconds + round)

	return sec, nanosec
}
