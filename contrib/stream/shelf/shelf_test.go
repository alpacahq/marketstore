package shelf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestShelf(t *testing.T) {
	t.Parallel()
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

		deadline := time.Now().Add(time.Millisecond)
		shelf.Store(tbk, genColumns(), &deadline)

		<-expC

		assert.True(t, expired)
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
		shelf.Store(tbk, genColumns(), &deadline)

		// replace
		shelf.Store(tbk, genColumns(), &deadline)

		<-expC

		assert.Equal(t, expireCount, 1)
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
		shelf.Store(tbk, genColumns(), &deadline)

		// replace
		replDeadline := deadline.Add(100 * time.Millisecond)
		shelf.Store(tbk, genColumns(), &replDeadline)

		<-expC

		assert.Equal(t, expireCount, 1)

		<-expC

		assert.Equal(t, expireCount, 2)
	}
	// attempted replacement w/ same deadline - make sure
	// things get properly cleaned up
	{
		h := NewShelfHandler(func(tbk io.TimeBucketKey, data interface{}) error {
			return nil
		})

		shelf := NewShelf(h)

		tbk := io.NewTimeBucketKey("AAPL/1D/OHLCV")

		deadline := time.Now().Add(24 * time.Hour).Truncate(24 * time.Hour)

		// store initial
		shelf.Store(tbk, genColumns(), &deadline)

		// attempt replace
		shelf.Store(tbk, genColumns(), &deadline)

		assert.Len(t, shelf.m, 1)
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
