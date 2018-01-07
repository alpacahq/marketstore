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
