package executor

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestTimeToIntervals(t *testing.T) {
	t.Parallel()

	t2 := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	index := io.TimeToIndex(t2, time.Minute)
	assert.Equal(t, index, int64(1))
	t2 = time.Date(2016, 12, 31, 23, 59, 0, 0, time.UTC)
	index = io.TimeToIndex(t2, time.Minute)
	assert.Equal(t, index, int64(366*1440))

	// 20161230 21:59:20 383000
	t1 := time.Date(2016, 12, 30, 21, 59, 20, 383000000, time.UTC)
	t.Log("LAL t1 = ", t1)

	// Check the 1Min interval
	utils.InstanceConfig.Timezone = time.UTC
	index = io.TimeToIndex(t1, time.Minute)

	o_t1 := io.IndexToTime(index, time.Minute, 2016)
	// fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	assert.Equal(t, o_t1.Hour(), 21)
	assert.Equal(t, o_t1.Minute(), 59)
	assert.Equal(t, o_t1.Second(), 0)

	o_t1 = io.IndexToTimeDepr(index, 1440, 2016)
	t.Log("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	assert.Equal(t, o_t1.Hour(), 21)
	assert.Equal(t, o_t1.Minute(), 59)
	assert.Equal(t, o_t1.Second(), 0)

	ticks := io.GetIntervalTicks32Bit(t1, index, 1440)
	t.Logf("Interval ticks = \t\t\t\t %d\n", int(ticks))

	seconds := t1.Second()
	nanos := t1.Nanosecond()
	fractionalSeconds := float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval := fractionalSeconds / 60.
	intervalTicks := uint32(fractionalInterval * math.MaxUint32)
	t.Logf("Manual calculation of interval ticks = \t\t %d\t%f\t%f\n", int(intervalTicks), fractionalSeconds, fractionalInterval)
	// Now let's build up a timestamp from the interval ticks
	fSec1 := 60. * (float64(intervalTicks) / float64(math.MaxUint32))
	fSec := 60. * (float64(ticks) / float64(math.MaxUint32))
	t.Logf("Fractional seconds from reconstruction: %f, from calc: %f\n", fSec1, fSec)
	assert.Equal(t, math.Abs(fSec-20.383) < 0.0000001, true)

	assert.Equal(t, math.Abs(float64(intervalTicks)-float64(ticks)) < 2., true)
}
