package calendar

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var NY, _ = time.LoadLocation("America/New_York")

type CalendarTestSuite struct{}

var _ = Suite(&CalendarTestSuite{})

func (s *CalendarTestSuite) TestCalendar(c *C) {
	// Weekend
	weekend := time.Date(2017, 1, 1, 11, 0, 0, 0, NY)
	c.Assert(Nasdaq.IsMarketOpen(weekend), Equals, false)
	c.Assert(Nasdaq.IsMarketDay(weekend), Equals, false)

	// MLK day 2018
	mlk := time.Date(2018, 1, 15, 11, 0, 0, 0, NY)
	c.Assert(Nasdaq.IsMarketOpen(mlk), Equals, false)
	c.Assert(Nasdaq.IsMarketDay(mlk), Equals, false)

	// July 3rd 2019 (early close)
	julThirdAM := time.Date(2018, 7, 3, 11, 0, 0, 0, NY)
	julThirdPM := time.Date(2018, 7, 3, 15, 0, 0, 0, NY)
	c.Assert(Nasdaq.IsMarketOpen(julThirdAM), Equals, true)
	c.Assert(Nasdaq.IsMarketDay(julThirdAM), Equals, true)

	c.Assert(Nasdaq.IsMarketOpen(julThirdPM), Equals, false)
	c.Assert(Nasdaq.IsMarketDay(julThirdPM), Equals, true)

	// normal day
	bestDayMid := time.Date(2021, 8, 31, 11, 0, 0, 0, NY)
	bestDayEarly := time.Date(2021, 8, 31, 7, 0, 0, 0, NY)
	bestDayLate := time.Date(2021, 8, 31, 19, 0, 0, 0, NY)
	c.Assert(Nasdaq.IsMarketOpen(bestDayMid), Equals, true)
	c.Assert(Nasdaq.IsMarketDay(bestDayMid), Equals, true)

	c.Assert(Nasdaq.IsMarketOpen(bestDayEarly), Equals, false)
	c.Assert(Nasdaq.IsMarketDay(bestDayEarly), Equals, true)

	c.Assert(Nasdaq.IsMarketOpen(bestDayLate), Equals, false)
	c.Assert(Nasdaq.IsMarketDay(bestDayLate), Equals, true)
}
