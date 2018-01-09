package trigger

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {}

func (s *TestSuite) TearDownSuite(c *C) {}

type EmptyTrigger struct{}

func (t *EmptyTrigger) Fire(keyPath string, offsets []int64) {
	// do nothing
}

func (s *TestSuite) TestMatch(c *C) {
	trig := &EmptyTrigger{}
	matcher := NewMatcher(trig, "*/1Min/OHLC")
	var matched bool
	matched = matcher.Match("TSLA/1Min/OHLC")
	c.Check(matched, Equals, true)
	matched = matcher.Match("TSLA/5Min/OHLC")
	c.Check(matched, Equals, false)
}
