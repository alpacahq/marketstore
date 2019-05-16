package writer

import (
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/tests"
	"github.com/alpacahq/marketstore/utils/io"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
)

var (
	May1st = time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)
	May2nd = time.Date(2019, 5, 2, 0, 0, 0, 0, time.UTC)
)

func TestQuotesWriterImpl_Write(t *testing.T) {
	// --- given ---
	m := &tests.MockMarketStoreWriter{}
	SUT := QuotesWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1Sec",
	}

	// 2 "Success" and 1 "RequestError" quotes data
	apiResponse := api.GetQuotesResponse{
		ArrayOfEquityQuote: []api.EquityQuote{
			{
				Outcome:  "Success",
				Security: &api.Security{Symbol: "1234"},
				Quote: &api.Quote{
					Ask:         123.4,
					Bid:         567.8,
					AskDateTime: api.XigniteDateTime(May1st),
					BidDateTime: api.XigniteDateTime(May2nd),
				},
			},
			{
				Outcome:  "Success",
				Security: &api.Security{Symbol: "5678"},
				Quote: &api.Quote{
					Ask:         90.1,
					Bid:         23.4,
					AskDateTime: api.XigniteDateTime(May2nd),
					BidDateTime: api.XigniteDateTime(May1st),
				},
			},
			{
				Outcome:  "RequestError",
				Security: &api.Security{Symbol: "9012"},
				Quote: &api.Quote{
					Ask:         123.4,
					Bid:         567.8,
					AskDateTime: api.XigniteDateTime(time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)),
					BidDateTime: api.XigniteDateTime(time.Date(2019, 5, 2, 0, 0, 0, 0, time.UTC)),
				},
			},},
	}

	// --- when ---
	err := SUT.Write(apiResponse)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// "Outcome" validation check
	if len(m.WrittenCSM) != 2 {
		t.Errorf("2 'Success' quotes should be written. len(m.WrittenCSM)=%v", len(m.WrittenCSM))
	}

	// Time Bucket Key Name check
	timeBucketKeyStr := string(m.WrittenCSM.GetMetadataKeys()[0].Key)
	if (timeBucketKeyStr != "1234/1Sec/TICK:"+io.DefaultTimeBucketSchema) {
		t.Errorf("TimeBucketKey for the first data is invalid. got=%v, want = %v",
			timeBucketKeyStr, "1234/1Sec/TICK:"+io.DefaultTimeBucketSchema)
	}

	// epoch time check
	epochTime := m.WrittenCSM[io.TimeBucketKey{Key: timeBucketKeyStr}].GetColumn("Epoch").([]int64)[0]
	epoch := time.Unix(epochTime, 0)
	if !epoch.Equal(May2nd) {
		t.Errorf("The newer of Ask and Bid Datetimes should be used for the Epoch column.")
	}
}
