package writer_test

import (
	"sort"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	exampleTrade = &api.Trade{
		Price:     1,
		Size:      2,
		Timestamp: time.Unix(3, 0),
	}
	exampleQuote = &api.Quote{
		BidPrice:  4,
		BidSize:   5,
		AskPrice:  6,
		AskSize:   7,
		Timestamp: time.Unix(8, 0),
	}
	exampleDailyBar = &api.Bar{
		Open:   9,
		High:   10,
		Low:    11,
		Close:  12,
		Volume: 13,
	}
	examplePreviousDailyBar = &api.Bar{
		Open:   14,
		High:   15,
		Low:    16,
		Close:  17,
		Volume: 18,
	}
	exampleMinuteBar = &api.Bar{
		Open:   19,
		High:   20,
		Low:    21,
		Close:  22,
		Volume: 23,
	}

	ny, _                  = time.LoadLocation("America/New_York")
	exampleCloseTimeQuote1 = &api.Quote{
		BidPrice:  1,
		Timestamp: time.Date(2019, 7, 19, 9, 29, 0, 0, ny), // close
	}
	exampleOpenTimeQuote1 = &api.Quote{
		BidPrice:  1,
		Timestamp: time.Date(2019, 7, 19, 9, 30, 0, 0, ny), // open
	}
	exampleOpenTimeQuote2 = &api.Quote{
		BidPrice:  2,
		Timestamp: time.Date(2019, 7, 19, 9, 31, 0, 0, ny), // open
	}
	timeChecker = feed.NewDefaultMarketTimeChecker(nil, nil,
		9, 30, 16, 30,
	)
)

func TestSnapshotWriterImpl_Write(t *testing.T) {
	t.Parallel()
	type fields struct {
		Timeframe   string
		Timezone    *time.Location
		TimeChecker writer.MarketTimeChecker
	}
	tests := []struct {
		name              string
		fields            fields
		snapshots         map[string]*api.Snapshot
		writeErr          error
		wantErr           bool
		wantTBKs          []io.TimeBucketKey
		wantCSMDataShapes []io.DataShape
		wantCSMLen        int
	}{
		{
			name: "OK/empty snapshot/snapshot with empty trade/quote is ignored",
			fields: fields{
				Timeframe:   "1Sec",
				Timezone:    time.UTC,
				TimeChecker: &writer.NoopMarketTimeChecker{},
			},
			snapshots: map[string]*api.Snapshot{
				"AAPL": {
					LatestTrade:  exampleTrade,
					LatestQuote:  exampleQuote,
					DailyBar:     exampleDailyBar,
					MinuteBar:    exampleMinuteBar,
					PrevDailyBar: examplePreviousDailyBar,
				},
				"AMZN": nil, // nil snapshot must be ignored
				"FB": {
					LatestTrade: nil, // snapshot with nil latestTrade must be ignored
					LatestQuote: exampleQuote,
				},
				"GOOG": {
					LatestTrade: exampleTrade,
					LatestQuote: nil, // snapshot with nil latestQuote must be ignored
				},
			},
			wantErr:  false,
			wantTBKs: []io.TimeBucketKey{*io.NewTimeBucketKey("AAPL/1Sec/TICK")},
			wantCSMDataShapes: []io.DataShape{
				{Name: "Epoch", Type: io.INT64},
				{Name: "QuoteTimestamp", Type: io.INT64},
				{Name: "Ask", Type: io.FLOAT32},
				{Name: "AskSize", Type: io.UINT32},
				{Name: "Bid", Type: io.FLOAT32},
				{Name: "BidSize", Type: io.UINT32},
				{Name: "TradeTimestamp", Type: io.INT64},
				{Name: "Price", Type: io.FLOAT32},
				{Name: "Size", Type: io.UINT32},
				{Name: "DailyTimestamp", Type: io.INT64},
				{Name: "Open", Type: io.FLOAT32},
				{Name: "High", Type: io.FLOAT32},
				{Name: "Low", Type: io.FLOAT32},
				{Name: "Close", Type: io.FLOAT32},
				{Name: "Volume", Type: io.UINT64},
				{Name: "MinuteTimestamp", Type: io.INT64},
				{Name: "MinuteOpen", Type: io.FLOAT32},
				{Name: "MinuteHigh", Type: io.FLOAT32},
				{Name: "MinuteLow", Type: io.FLOAT32},
				{Name: "MinuteClose", Type: io.FLOAT32},
				{Name: "MinuteVolume", Type: io.UINT64},
				{Name: "PreviousTimestamp", Type: io.INT64},
				{Name: "PreviousOpen", Type: io.FLOAT32},
				{Name: "PreviousHigh", Type: io.FLOAT32},
				{Name: "PreviousLow", Type: io.FLOAT32},
				{Name: "PreviousClose", Type: io.FLOAT32},
				{Name: "PreviousVolume", Type: io.UINT64},
			},
			wantCSMLen: 1,
		},
		{
			name: "OK/records in off-hour time must be dropped (extended_hours:false)",
			fields: fields{
				Timeframe:   "1Sec",
				Timezone:    time.UTC,
				TimeChecker: timeChecker,
			},
			snapshots: map[string]*api.Snapshot{
				"AAPL": {LatestTrade: exampleTrade, LatestQuote: exampleCloseTimeQuote1},
				"AMZN": {LatestTrade: exampleTrade, LatestQuote: exampleOpenTimeQuote1},
				"FB":   {LatestTrade: exampleTrade, LatestQuote: exampleOpenTimeQuote2},
			},
			wantErr: false,
			wantTBKs: []io.TimeBucketKey{
				*io.NewTimeBucketKey("AMZN/1Sec/TICK"),
				*io.NewTimeBucketKey("FB/1Sec/TICK"),
			},
			wantCSMDataShapes: []io.DataShape{
				{Name: "Epoch", Type: io.INT64},
				{Name: "QuoteTimestamp", Type: io.INT64},
				{Name: "Ask", Type: io.FLOAT32},
				{Name: "AskSize", Type: io.UINT32},
				{Name: "Bid", Type: io.FLOAT32},
				{Name: "BidSize", Type: io.UINT32},
				{Name: "TradeTimestamp", Type: io.INT64},
				{Name: "Price", Type: io.FLOAT32},
				{Name: "Size", Type: io.UINT32},
				{Name: "DailyTimestamp", Type: io.INT64},
				{Name: "Open", Type: io.FLOAT32},
				{Name: "High", Type: io.FLOAT32},
				{Name: "Low", Type: io.FLOAT32},
				{Name: "Close", Type: io.FLOAT32},
				{Name: "Volume", Type: io.UINT64},
				{Name: "MinuteTimestamp", Type: io.INT64},
				{Name: "MinuteOpen", Type: io.FLOAT32},
				{Name: "MinuteHigh", Type: io.FLOAT32},
				{Name: "MinuteLow", Type: io.FLOAT32},
				{Name: "MinuteClose", Type: io.FLOAT32},
				{Name: "MinuteVolume", Type: io.UINT64},
				{Name: "PreviousTimestamp", Type: io.INT64},
				{Name: "PreviousOpen", Type: io.FLOAT32},
				{Name: "PreviousHigh", Type: io.FLOAT32},
				{Name: "PreviousLow", Type: io.FLOAT32},
				{Name: "PreviousClose", Type: io.FLOAT32},
				{Name: "PreviousVolume", Type: io.UINT64},
			},
			wantCSMLen: 2, // AAPL record is dropped because it's off-hours
		},
		{
			name: "NG/failed to write to marketstore",
			fields: fields{
				Timeframe:   "1Sec",
				Timezone:    time.UTC,
				TimeChecker: &writer.NoopMarketTimeChecker{},
			},
			snapshots: map[string]*api.Snapshot{
				"AAPL": {
					LatestTrade: exampleTrade,
					LatestQuote: exampleQuote,
				},
			},
			writeErr: errors.New("error"),
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msw := &internal.MockMarketStoreWriter{Err: tt.writeErr}

			q := writer.NewSnapshotWriterImpl(msw, tt.fields.Timeframe, tt.fields.Timezone, tt.fields.TimeChecker)
			err := q.Write(tt.snapshots)
			require.Equal(t, tt.wantErr, err != nil)

			tbks := msw.WrittenCSM.GetMetadataKeys()
			if tt.wantTBKs != nil {
				// sort tbks to ignore the order of keys
				sort.SliceStable(tbks, func(i, j int) bool {
					return tbks[i].String() < tbks[j].String()
				})
				require.Equal(t, tt.wantTBKs, tbks)
			}

			if len(tt.wantCSMDataShapes) > 0 {
				require.Equal(t, tt.wantCSMDataShapes, msw.WrittenCSM[tbks[0]].GetDataShapes())
			}
		})
	}
}
