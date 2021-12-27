package orderbook

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderBook(t *testing.T) {
	t.Parallel()
	ob := NewOrderBook()

	{
		b, a := ob.BBO()
		assert.Equal(t, float32(0.0), b.Price)
		assert.Equal(t, float32(0.0), a.Price)
	}

	bids0 := []Entry{
		{153.51, 200},
		{153.66, 100},
		{153.67, 100},
		{153.71, 100},
		{153.72, 100},
	}
	for _, bid := range bids0 {
		ob.Bid(bid)
	}

	{
		b, a := ob.BBO()
		assert.Equal(t, float32(153.72), b.Price)
		assert.Equal(t, float32(0.0), a.Price)
	}

	asks0 := []Entry{
		{154.19, 100},
		{154.09, 100},
		{154.05, 100},
		{154.04, 100},
	}
	for _, ask := range asks0 {
		ob.Ask(ask)
	}

	{
		b, a := ob.BBO()
		assert.Equal(t, float32(153.72), b.Price)
		assert.Equal(t, int32(100), b.Size)
		assert.Equal(t, float32(154.04), a.Price)
		assert.Equal(t, int32(100), a.Size)
	}

	asks1 := []Entry{
		{154.05, 200},
		{154.04, 0},
	}
	for _, ask := range asks1 {
		ob.Ask(ask)
	}

	{
		_, a := ob.BBO()
		assert.Equal(t, float32(154.05), a.Price)
		assert.Equal(t, int32(200), a.Size)
	}
}
