package writer

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/internal"
	"github.com/alpacahq/marketstore/utils/io"
)

func TestQuotesRangeWriterImpl_Write(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := QuotesRangeWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1D",
	}

	// 2 quotes data
	apiResponse := api.GetQuotesRangeResponse{
		Security: &api.Security{Symbol: "1234"},
		ArrayOfEndOfDayQuote: []api.EndOfDayQuote{
			{
				Date:   api.XigniteDay(May1st),
				Open:   12.3,
				Close:  45.6,
				High:   78.9,
				Low:    0.12,
				Volume: 100,
			},
			{
				Date:   api.XigniteDay(May2nd),
				Open:   1.2,
				Close:  3.4,
				High:   5.6,
				Low:    7.8,
				Volume: 100,
			},
			// When Volume is 0, xignite getQuotesRange API returns data with {open:0, close:0, high:0, low:0}.
			// we don't write the zero data to marketstore.
			{
				Date:   api.XigniteDay(May3rd),
				Open:   0.0,
				Close:  0.0,
				High:   0.0,
				Low:    0.0,
				Volume: 0,
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse.Security.Symbol, apiResponse.ArrayOfEndOfDayQuote, false)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// 2 quotes data is stored to the marketstore by 1 CSM.
	// 1 quotes data out of 3 is ignored because it's zero data (= {open:0, close:0, high:0, low:0} )
	if len(m.WrittenCSM) != 1 {
		t.Errorf("quotes should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}

	// Time Bucket Key Name check
	timeBucketKeyStr := string(m.WrittenCSM.GetMetadataKeys()[0].Key)
	if timeBucketKeyStr != "1234/1D/OHLCV:"+io.DefaultTimeBucketSchema {
		t.Errorf("TimeBucketKey name is invalid. got=%v, want = %v",
			timeBucketKeyStr, "1234/1D/OHLCV:"+io.DefaultTimeBucketSchema)
	}
}

func TestQuotesRangeWriterImpl_noDataToWrite(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := QuotesRangeWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1D",
	}

	// all data are Volume=0 and not necessary to be written
	apiResponse := api.GetQuotesRangeResponse{
		Security: &api.Security{Symbol: "1234"},
		ArrayOfEndOfDayQuote: []api.EndOfDayQuote{
			// When Volume is 0, xignite getQuotesRange API returns data with {open:0, close:0, high:0, low:0}.
			// we don't write the zero data to marketstore.
			{
				Date:   api.XigniteDay(May3rd),
				Open:   0.0,
				Close:  0.0,
				High:   0.0,
				Low:    0.0,
				Volume: 0,
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse.Security.Symbol, apiResponse.ArrayOfEndOfDayQuote, false)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}
}

func TestQuotesRangeWriterImpl_getLatestTime(t *testing.T) {
	// --- given ---
	t1 := time.Date(2019, 05, 01, 12, 34, 56, 0, time.UTC)
	t2 := time.Date(2018, 05, 01, 12, 34, 56, 0, time.UTC)
	t3 := time.Date(2017, 05, 01, 12, 34, 56, 0, time.UTC)

	// --- when ---
	lt := getLatestTime(t1, t2, t3)
	lt2 := getLatestTime(t2, t3, t1)
	lt3 := getLatestTime(t3, t1, t2)

	// --- then ---
	// the latest time should be always t1
	if (lt != t1) || (lt2 != t1) || (lt3 != t1) {
		t.Fatal("latest time should be retrieved")
	}
}
