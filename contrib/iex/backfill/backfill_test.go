package main

import (
	"testing"
	"time"

	"github.com/timpalpant/go-iex/iextp/tops"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{})

type TestSuite struct{}

func (s *TestSuite) TestMakeBars(c *C) {
	trades := []*tops.TradeReportMessage{
		{
			Symbol:    "SPY",
			Timestamp: time.Date(2018, 12, 21, 10, 30, 1, 0, NY),
			Price:     200.00,
			Size:      100,
		},
		{
			Symbol:    "SPY",
			Timestamp: time.Date(2018, 12, 21, 10, 30, 15, 0, NY),
			Price:     201.00,
			Size:      100,
		},
		{
			Symbol:    "SPY",
			Timestamp: time.Date(2018, 12, 21, 10, 30, 15, 103, NY),
			Price:     199.55,
			Size:      40,
		},
		{
			Symbol:    "AAPL",
			Timestamp: time.Date(2018, 12, 21, 10, 30, 23, 420356, NY),
			Price:     159.55,
			Size:      400,
		},
	}

	openTime := trades[0].Timestamp.Truncate(time.Minute)
	closeTime := openTime.Add(time.Minute)
	symBars := makeSymBars(trades, openTime, closeTime)
	c.Assert(len(symBars), Equals, 2)
	c.Assert(symBars["SPY"].Volume, Equals, int64(240))
	c.Assert(symBars["SPY"].Open, Equals, float64(200.00))
	c.Assert(symBars["SPY"].Close, Equals, float64(199.55))
	c.Assert(symBars["AAPL"].Low, Equals, float64(159.55))
	c.Assert(symBars["AAPL"].Symbol, Equals, "AAPL")
}
