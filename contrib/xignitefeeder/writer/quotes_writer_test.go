package writer

import (
	"sort"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/internal"
	"github.com/alpacahq/marketstore/v4/utils/io"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
)

var (
	May1st         = time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)
	May2nd         = time.Date(2019, 5, 2, 0, 0, 0, 0, time.UTC)
	May3rd         = time.Date(2019, 5, 3, 0, 0, 0, 0, time.UTC)
	LastMarketDate = time.Date(2019, 5, 4, 0, 0, 0, 0, time.UTC)
)

func TestQuotesWriterImpl_Write(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := QuotesWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1Sec",
		Timezone:          time.UTC,
	}

	// 2 "Success" and 1 "RequestError" quotes data
	apiResponse := api.GetQuotesResponse{
		ArrayOfEquityQuote: []api.EquityQuote{
			{
				Outcome:  "Success",
				Security: &api.Security{Symbol: "1234"},
				Quote: &api.Quote{
					Ask:            123.4,
					Bid:            567.8,
					AskDateTime:    api.XigniteDateTime(May1st),
					BidDateTime:    api.XigniteDateTime(May2nd),
					LastMarketDate: api.XigniteDay(LastMarketDate),
				},
			},
			{
				Outcome:  "Success",
				Security: &api.Security{Symbol: "5678"},
				Quote: &api.Quote{
					Ask:            90.1,
					Bid:            23.4,
					AskDateTime:    api.XigniteDateTime(May2nd),
					BidDateTime:    api.XigniteDateTime(May1st),
					LastMarketDate: api.XigniteDay(LastMarketDate),
				},
			},
			{
				Outcome:  "RequestError",
				Security: &api.Security{Symbol: "9012"},
				Quote: &api.Quote{
					Ask:            123.4,
					Bid:            567.8,
					AskDateTime:    api.XigniteDateTime(time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC)),
					BidDateTime:    api.XigniteDateTime(time.Date(2019, 5, 2, 0, 0, 0, 0, time.UTC)),
					LastMarketDate: api.XigniteDay(LastMarketDate),
				},
			}},
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
	keys := m.WrittenCSM.GetMetadataKeys()
	keyStrings := make([]string, len(keys))
	for i, key := range keys {
		keyStrings[i] = string(key.Key)
	}
	sort.Strings(keyStrings)
	timeBucketKeyStr := keyStrings[0]
	if timeBucketKeyStr != "1234/1Sec/TICK:"+io.DefaultTimeBucketSchema {
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

//  UTCOffset response parameter is used to convert the time in API response to UTC.
func TestQuotesWriterImpl_TimeLocation(t *testing.T) {
	// --- given ---
	m := &internal.MockMarketStoreWriter{}
	SUT := QuotesWriterImpl{
		MarketStoreWriter: m,
		Timeframe:         "1Sec",
		Timezone:          time.UTC,
	}

	// data with UTCOffset
	apiResponse := api.GetQuotesResponse{
		ArrayOfEquityQuote: []api.EquityQuote{
			{
				Outcome:  "Success",
				Security: &api.Security{Symbol: "1234"},
				Quote: &api.Quote{
					Ask:            123.4,
					Bid:            567.8,
					AskDateTime:    api.XigniteDateTime(May1st),
					BidDateTime:    api.XigniteDateTime(May1st),
					UTCOffSet:      3, // which means the datetime is UTC+3:00
					LastMarketDate: api.XigniteDay(LastMarketDate),
				},
			},
		},
	}

	// --- when ---
	err := SUT.Write(apiResponse)

	// --- then ---
	if err != nil {
		t.Fatalf("error should be nil. got=%v", err)
	}

	// Time Bucket Key
	key := m.WrittenCSM.GetMetadataKeys()[0].Key

	// epoch time check
	epochTime := m.WrittenCSM[io.TimeBucketKey{Key: key}].GetColumn("Epoch").([]int64)[0]
	epoch := time.Unix(epochTime, 0)
	if !epoch.Equal(May1st.Add(-3 * time.Hour)) { // = AskDateTime - UTCOffset
		t.Errorf("Epoch value should be considered the UTCOffset.")
	}
}
