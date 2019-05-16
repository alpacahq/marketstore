package writer

import (
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/tests"
	"github.com/alpacahq/marketstore/utils/io"
	"testing"
)

func TestQuotesRangeWriterImpl_Write(t *testing.T) {
	// --- given ---
	m := &tests.MockMarketStoreWriter{}
	SUT := QuotesRangeWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1D",
	}

	// 2 quotes data
	apiResponse := api.GetQuotesRangeResponse{
		Security: &api.Security{Symbol: "1234"},
		ArrayOfEndOfDayQuote: []api.EndOfDayQuote{
			{
				Date:   configs.CustomDay(May1st),
				Open:   12.3,
				Close:  45.6,
				High:   78.9,
				Low:    0.12,
				Volume: 100,
			},
			{
				Date:   configs.CustomDay(May2nd),
				Open:   1.2,
				Close:  3.4,
				High:   5.6,
				Low:    7.8,
				Volume: 100,
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// 2 quotes data is stored to the marketstore by 1 CSM.
	if len(m.WrittenCSM) != 1 {
		t.Errorf("quotes should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}

	// Time Bucket Key Name check
	timeBucketKeyStr := string(m.WrittenCSM.GetMetadataKeys()[0].Key)
	if (timeBucketKeyStr != "1234/1D/OHLCV:"+io.DefaultTimeBucketSchema) {
		t.Errorf("TimeBucketKey name is invalid. got=%v, want = %v",
			timeBucketKeyStr, "1234/1D/OHLCV:"+io.DefaultTimeBucketSchema)
	}
}
