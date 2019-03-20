package loader

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type LoaderTests struct{}

var _ = Suite(&LoaderTests{})

func (s *LoaderTests) SetUpSuite(c *C)    {}
func (s *LoaderTests) TearDownSuite(c *C) {}

func (s *LoaderTests) TestParseTime(c *C) {
	tt := time.Date(2016, 12, 30, 21, 59, 20, 383000000, time.UTC)
	var fAdj int
	timeFormat := "20060102 15:04:05"
	dateTime := "20161230 21:59:20 383000"
	tzLoc := time.UTC
	tTest, err := parseTime(timeFormat, dateTime, tzLoc, fAdj)
	c.Assert(err != nil, Equals, true)
	formatAdj := len(dateTime) - len(timeFormat)
	tTest, err = parseTime(timeFormat, dateTime, tzLoc, formatAdj)
	c.Assert(tt == tTest, Equals, true)
}

func (s *LoaderTests) TestParseTimestamp(c *C) {
	tt := time.Date(2017, 11, 07, 07, 8, 23, 383000000, time.UTC)
	var fAdj int
	timeFormat := "timestamp"
	dateTime := "1510038503.383"
	tzLoc := time.UTC
	tTest, err := parseTime(timeFormat, dateTime, tzLoc, fAdj)
	c.Assert(err == nil, Equals, true)
	c.Assert(tt == tTest, Equals, true)

	tt1 := time.Date(2017, 11, 07, 07, 8, 23, 0, time.UTC)
	dateTime = "1510038503"
	tTest, err = parseTime(timeFormat, dateTime, tzLoc, fAdj)
	c.Assert(err == nil, Equals, true)
	c.Assert(tt1 == tTest, Equals, true)
}
