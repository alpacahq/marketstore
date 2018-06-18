package shelf

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/utils/io"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type ShelfTestSuite struct{}

var _ = Suite(&ShelfTestSuite{})

func (s *ShelfTestSuite) TestShelf(c *C) {
	// normal expiration
	{
		expC := make(chan struct{}, 1)
		expired := false
		h := NewShelfHandler(func(tbk io.TimeBucketKey, data interface{}) error {
			expired = true
			expC <- struct{}{}
			return nil
		})

		shelf := NewShelf(h)

		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")

		shelf.Store(tbk, genColumns(), time.Now().Add(time.Millisecond))

		<-expC

		c.Assert(expired, Equals, true)
	}
	// replacement with same deadline, then expiration
	{
		expC := make(chan struct{}, 1)
		expireCount := 0
		h := NewShelfHandler(func(tbk io.TimeBucketKey, data interface{}) error {
			expireCount++
			expC <- struct{}{}
			return nil
		})

		shelf := NewShelf(h)

		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")

		deadline := time.Now().Add(100 * time.Millisecond)

		// store initial
		shelf.Store(tbk, genColumns(), deadline)

		// replace
		shelf.Store(tbk, genColumns(), deadline)

		<-expC

		c.Assert(expireCount, Equals, 1)
	}
	// replacement with new deadline, then expiration
	{
		expC := make(chan struct{}, 2)

		expireCount := 0
		h := NewShelfHandler(func(tbk io.TimeBucketKey, data interface{}) error {
			expireCount++
			expC <- struct{}{}

			return nil
		})

		shelf := NewShelf(h)

		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")

		deadline := time.Now().Add(100 * time.Millisecond)

		// store initial
		shelf.Store(tbk, genColumns(), deadline)

		// replace
		shelf.Store(tbk, genColumns(), deadline.Add(100*time.Millisecond))

		<-expC

		c.Assert(expireCount, Equals, 1)

		<-expC

		c.Assert(expireCount, Equals, 2)
	}
}

func genColumns() map[string]interface{} {
	return map[string]interface{}{
		"Open":   float32(1.0),
		"High":   float32(2.0),
		"Low":    float32(0.5),
		"Close":  float32(1.5),
		"Volume": int32(10),
		"Epoch":  int64(123456789),
	}
}
