package executor

import (
	"fmt"
	"math"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	. "gopkg.in/check.v1"
)

var _ = Suite(&TickTests{})

type TickTests struct{}

func (s *TickTests) SetUpSuite(c *C)    {}
func (s *TickTests) TearDownSuite(c *C) {}

func (s *TickTests) TestTimeToIntervals(c *C) {
	t2 := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	index := io.TimeToIndex(t2, time.Minute)
	c.Assert(index == 1, Equals, true)
	t2 = time.Date(2016, 12, 31, 23, 59, 0, 0, time.UTC)
	index = io.TimeToIndex(t2, time.Minute)
	c.Assert(index == 366*1440, Equals, true)

	//20161230 21:59:20 383000
	t1 := time.Date(2016, 12, 30, 21, 59, 20, 383000000, time.UTC)
	fmt.Println("LAL t1 = ", t1)

	// Check the 1Min interval
	utils.InstanceConfig.Timezone = time.UTC
	index = io.TimeToIndex(t1, time.Minute)

	o_t1 := io.IndexToTime(index, time.Minute, 2016)
	//fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	c.Assert(o_t1.Hour(), Equals, 21)
	c.Assert(o_t1.Minute(), Equals, 59)
	c.Assert(o_t1.Second(), Equals, 0)

	o_t1 = io.IndexToTimeDepr(index, 1440, 2016)
	fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	c.Assert(o_t1.Hour(), Equals, 21)
	c.Assert(o_t1.Minute(), Equals, 59)
	c.Assert(o_t1.Second(), Equals, 0)

	ticks := io.GetIntervalTicks32Bit(t1, index, 1440)
	fmt.Printf("Interval ticks = \t\t\t\t %d\n", int(ticks))

	seconds := t1.Second()
	nanos := t1.Nanosecond()
	fractionalSeconds := float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval := fractionalSeconds / 60.
	intervalTicks := uint32(fractionalInterval * math.MaxUint32)
	fmt.Printf("Manual calculation of interval ticks = \t\t %d\t%f\t%f\n", int(intervalTicks), fractionalSeconds, fractionalInterval)
	// Now let's build up a timestamp from the interval ticks
	fSec1 := 60. * (float64(intervalTicks) / float64(math.MaxUint32))
	fSec := 60. * (float64(ticks) / float64(math.MaxUint32))
	fmt.Printf("Fractional seconds from reconstruction: %f, from calc: %f\n", fSec1, fSec)
	c.Assert(math.Abs(fSec-20.383) < 0.0000001, Equals, true)

	c.Assert(math.Abs(float64(intervalTicks)-float64(ticks)) < 2., Equals, true)
}
