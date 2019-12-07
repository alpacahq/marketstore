package writer

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/internal"
	"github.com/alpacahq/marketstore/utils/io"
)

var (
	BarStartTime1 = time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)
	BarEndTime1   = time.Date(2019, 5, 1, 0, 5, 0, 0, time.UTC)
	BarStartTime2 = time.Date(2019, 5, 1, 0, 5, 0, 0, time.UTC)
	BarEndTime2   = time.Date(2019, 5, 1, 0, 10, 0, 0, time.UTC)
	BarStartTime3 = time.Date(2019, 5, 1, 0, 10, 0, 0, time.UTC)
	BarEndTime3   = time.Date(2019, 5, 1, 0, 15, 0, 0, time.UTC)
)

func TestBarWriterImpl_Write(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := BarWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "5Min",
		Timezone:          time.UTC,
	}

	// 2 quotes data
	apiResponse := api.GetBarsResponse{
		Security: &api.Security{Symbol: "1234"},
		ArrayOfBar: []api.Bar{
			{
				StartDateTime: api.XigniteDateTime(BarStartTime1),
				EndDateTime:   api.XigniteDateTime(BarEndTime1),
				Open:          12.3,
				Close:         45.6,
				High:          78.9,
				Low:           0.12,
				Volume:        100,
			},
			{
				StartDateTime: api.XigniteDateTime(BarStartTime2),
				EndDateTime:   api.XigniteDateTime(BarEndTime2),
				Open:          1.2,
				Close:         3.4,
				High:          5.6,
				Low:           7.8,
				Volume:        100,
			},
			// When Volume is 0, xignite getQuotesRange API returns data with {open:0, close:0, high:0, low:0}.
			// we don't write the zero data to marketstore.
			{
				StartDateTime: api.XigniteDateTime(BarStartTime3),
				EndDateTime:   api.XigniteDateTime(BarEndTime3),
				Open:          0.0,
				Close:         0.0,
				High:          0.0,
				Low:           0.0,
				Volume:        0,
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse.Security.Symbol, apiResponse.ArrayOfBar, false)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// 2 quotes data are stored to the marketstore by 1 CSM.
	// 1 quotes data out of 3 is ignored because it's zero data (= {open:0, close:0, high:0, low:0} )
	if len(m.WrittenCSM) != 1 {
		t.Errorf("bar data should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}

	// Time Bucket Key Name check
	timeBucketKeyStr := string(m.WrittenCSM.GetMetadataKeys()[0].Key)
	if timeBucketKeyStr != "1234/5Min/OHLCV:"+io.DefaultTimeBucketSchema {
		t.Errorf("TimeBucketKey name is invalid. got=%v, want = %v",
			timeBucketKeyStr, "1234/5Min/OHLCV:"+io.DefaultTimeBucketSchema)
	}
}

func TestBarWriterImpl_Write_IndexSymbol(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := BarWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "5Min",
		Timezone:          time.UTC,
	}

	// data with Volume=0
	apiResponse := api.GetBarsResponse{
		Security: &api.Security{Symbol: "123"},
		ArrayOfBar: []api.Bar{
			// if the symbols is an index symbol, data with Volume=0 are also written to marketstore.
			{
				StartDateTime: api.XigniteDateTime(BarStartTime3),
				EndDateTime:   api.XigniteDateTime(BarEndTime3),
				Open:          0.0,
				Close:         0.0,
				High:          0.0,
				Low:           0.0,
				Volume:        0,
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse.Security.Symbol, apiResponse.ArrayOfBar, true)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	if len(m.WrittenCSM) != 1 {
		t.Errorf("bar data with Volume=0 should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}
}
