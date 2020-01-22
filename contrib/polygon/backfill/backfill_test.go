package backfill

import (
	"fmt"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/utils/io"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&BackfillTests{})

type BackfillTests struct{}

func (s *BackfillTests) SetUpSuite(c *C)    {}
func (s *BackfillTests) TearDownSuite(c *C) {}

func (s *BackfillTests) TestTicksToBars(c *C) {
	NY, _ := time.LoadLocation("America/New_York")
	// Given a set of TradeTicks from three exchanges, a symbol and limited set of exchanges
	ticks := []api.TradeTick{
		// Timestamp  int64   `json:"t"`
		// Price      float64 `json:"p"`
		// Size       int     `json:"s"`
		// Exchange   string  `json:"e"`
		// Condition1 int     `json:"c1"`
		// Condition2 int     `json:"c2"`
		// Condition3 int     `json:"c3"`
		// Condition4 int     `json:"c4"`
		{
			Timestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano() / 1e6,
			Price:     300,
			Size:      100,
			Exchange:  "9",
		},
		{
			Timestamp: time.Date(2020, 1, 21, 9, 30, 1, 0, NY).UnixNano() / 1e6,
			Price:     299.9,
			Size:      50,
			Exchange:  "8",
		},
		{
			Timestamp: time.Date(2020, 1, 21, 9, 30, 3, 0, NY).UnixNano() / 1e6,
			Price:     300.1,
			Size:      80,
			Exchange:  "17",
		},
	}
	symbol := "AAPL"
	exchangeIDs := []string{"9", "17"}
	key := io.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")

	// When we call tradesToBars
	csm := tradesToBars(ticks, symbol, exchangeIDs)
	c.Assert(csm, NotNil)
	c.Assert(csm[*key].GetColumn("Open").([]float32), DeepEquals, []float32{300})
	c.Assert(csm[*key].GetColumn("High").([]float32), DeepEquals, []float32{300.1})
	c.Assert(csm[*key].GetColumn("Low").([]float32), DeepEquals, []float32{300})
	c.Assert(csm[*key].GetColumn("Close").([]float32), DeepEquals, []float32{300.1})
	c.Assert(csm[*key].GetColumn("Volume").([]int32), DeepEquals, []int32{180})

	{
		csm := tradesToBars(ticks, symbol, []string{"8", "9"})
		c.Assert(csm, NotNil)
		fmt.Printf("%v\n", csm[*key].GetTime()[0])
		c.Assert(csm[*key].GetTime(), DeepEquals, []time.Time{
			time.Date(2020, 1, 21, 9, 30, 0, 0, NY).In(time.UTC),
		})
		c.Assert(csm[*key].GetColumn("Open").([]float32), DeepEquals, []float32{300})
		c.Assert(csm[*key].GetColumn("High").([]float32), DeepEquals, []float32{300})
		c.Assert(csm[*key].GetColumn("Low").([]float32), DeepEquals, []float32{299.9})
		c.Assert(csm[*key].GetColumn("Close").([]float32), DeepEquals, []float32{299.9})
		c.Assert(csm[*key].GetColumn("Volume").([]int32), DeepEquals, []int32{150})
	}
	// c.Assert(csm. )
	// Then the returned ColumnSeriesMarks should contain data from the two
	// specified exchanges, accumulated to minutes

	/*
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
	*/
}
