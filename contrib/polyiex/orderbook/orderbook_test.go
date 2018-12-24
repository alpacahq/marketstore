package orderbook

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type OrderBookTestSuite struct{}

var _ = Suite(&OrderBookTestSuite{})

func (s *OrderBookTestSuite) TestOrderBook(c *C) {
	ob := NewOrderBook()

	{
		b, a := ob.BBO()
		c.Assert(float32(0.0), Equals, b.Price)
		c.Assert(float32(0.0), Equals, a.Price)
	}

	bids0 := []Entry{
		{154.19, 100},
		{154.09, 100},
		{154.05, 100},
		{154.04, 100},
	}
	for _, bid := range bids0 {
		ob.Bid(bid)
	}

	{
		b, a := ob.BBO()
		c.Assert(float32(154.04), Equals, b.Price)
		c.Assert(float32(0.0), Equals, a.Price)
	}

	asks0 := []Entry{
		{153.51, 200},
		{153.66, 100},
		{153.67, 100},
		{153.71, 100},
		{153.72, 100},
	}
	for _, ask := range asks0 {
		ob.Ask(ask)
	}

	{
		b, a := ob.BBO()
		c.Assert(float32(154.04), Equals, b.Price)
		c.Assert(int32(100), Equals, b.Size)
		c.Assert(float32(153.72), Equals, a.Price)
		c.Assert(int32(100), Equals, a.Size)
	}

	bids1 := []Entry{
		{154.05, 200},
		{154.04, 0},
	}
	for _, bid := range bids1 {
		ob.Bid(bid)
	}

	{
		b, _ := ob.BBO()
		c.Assert(float32(154.05), Equals, b.Price)
		c.Assert(int32(200), Equals, b.Size)
	}

}
