package utils

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

type UtilsTestSuite struct{}

var _ = Suite(&UtilsTestSuite{})

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func (s *UtilsTestSuite) TestTimeframeFromDuration(c *C) {
	tf := TimeframeFromDuration(22 * time.Minute)
	c.Assert(tf.String, Equals, "22Min")
	c.Assert(tf.Duration, Equals, 22*time.Minute)

	tf = TimeframeFromDuration(time.Hour)
	c.Assert(tf.String, Equals, "1H")
	c.Assert(tf.Duration, Equals, time.Hour)

	tf = TimeframeFromDuration(time.Nanosecond)
	c.Assert(tf, IsNil)

	tf = TimeframeFromDuration(5 * Year)
	c.Assert(tf, IsNil)
}

func (s *UtilsTestSuite) TestTimeframeFromString(c *C) {
	tf := TimeframeFromString("15H")
	c.Assert(tf.String, Equals, "15H")
	c.Assert(tf.Duration, Equals, 15*time.Hour)

	tf = TimeframeFromString("xyz")
	c.Assert(tf, IsNil)

	tf = TimeframeFromString("0H")
	c.Assert(tf, IsNil)
}

func (s *UtilsTestSuite) TestCandleDuration(c *C) {
	var cd *CandleDuration
	var val, start time.Time
	var within bool
	cd = CandleDurationFromString("5Min")
	val = time.Date(2017, 9, 10, 13, 47, 0, 0, time.UTC)
	c.Assert(cd.Truncate(val), Equals, time.Date(2017, 9, 10, 13, 45, 0, 0, time.UTC))
	c.Assert(cd.Ceil(val), Equals, time.Date(2017, 9, 10, 13, 50, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2017, 9, 10, 13, 46, 0, 0, time.UTC), start)
	c.Assert(within, Equals, true)
	within = cd.IsWithin(time.Date(2017, 9, 10, 13, 51, 0, 0, time.UTC), start)
	c.Assert(within, Equals, false)

	cd = CandleDurationFromString("1M")
	val = time.Date(2017, 9, 10, 13, 47, 0, 0, time.UTC)
	c.Assert(cd.Truncate(val), Equals, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC))
	c.Assert(cd.Ceil(val), Equals, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2017, 9, 26, 0, 0, 0, 0, time.UTC), start)
	c.Assert(within, Equals, true)
	within = cd.IsWithin(time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), start)
	c.Assert(within, Equals, false)
	within = cd.IsWithin(time.Date(2017, 8, 31, 0, 0, 0, 0, time.UTC), start)
	c.Assert(within, Equals, false)

	val = time.Date(2017, 12, 10, 13, 47, 0, 0, time.UTC)
	c.Assert(cd.Truncate(val), Equals, time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC))
	c.Assert(cd.Ceil(val), Equals, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2018, 1, 26, 0, 0, 0, 0, time.UTC), start)
	c.Assert(within, Equals, false)
	within = cd.IsWithin(time.Date(2016, 12, 10, 0, 0, 0, 0, time.UTC), start)
	c.Assert(within, Equals, false)

	cd = CandleDurationFromString("1W")
	val = time.Date(2017, 1, 8, 0, 0, 0, 0, time.UTC)
	start = time.Date(2018, 1, 7, 0, 0, 0, 0, time.UTC)
	within = cd.IsWithin(val, start)
	c.Assert(within, Equals, false)
	val = time.Date(2018, 1, 8, 0, 0, 0, 0, time.UTC)
	start = time.Date(2018, 1, 8, 0, 0, 0, 0, time.UTC)
	within = cd.IsWithin(val, start)
	c.Assert(within, Equals, true)

}
