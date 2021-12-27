package writer

import (
	"testing"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	barTimestamp1 = time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)
	barTimestamp2 = time.Date(2019, 5, 1, 0, 5, 0, 0, time.UTC)
	barTimestamp3 = time.Date(2019, 5, 1, 0, 10, 0, 0, time.UTC)
)

func TestBarWriterImpl_Write(t *testing.T) {
	t.Parallel()
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := BarWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "5Min",
		Timezone:          time.UTC,
	}

	// 2 bar data
	symbol := "1234"
	bars := []alpaca.Bar{
		{
			Time:   barTimestamp1.Unix(),
			Open:   12.3,
			Close:  45.6,
			High:   78.9,
			Low:    0.12,
			Volume: 100,
		},
		{
			Time:   barTimestamp2.Unix(),
			Open:   1.2,
			Close:  3.4,
			High:   5.6,
			Low:    7.8,
			Volume: 100,
		},
	}

	// --- when ---
	err := SUT.Write(symbol, bars)
	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// 2 bar data are stored to the marketstore by 1 CSM.
	if len(m.WrittenCSM) != 1 {
		t.Errorf("bar data should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}

	// Time Bucket Key Name check
	timeBucketKeyStr := m.WrittenCSM.GetMetadataKeys()[0].String()
	if timeBucketKeyStr != "1234/5Min/OHLCV:"+io.DefaultTimeBucketSchema {
		t.Errorf("TimeBucketKey name is invalid. got=%v, want = %v",
			timeBucketKeyStr, "1234/5Min/OHLCV:"+io.DefaultTimeBucketSchema)
	}
}
