package writer

import (
	"testing"
	"time"

	v2 "github.com/alpacahq/alpaca-trade-api-go/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	exampleTrade = &v2.Trade{
		Price:     1,
		Size:      2,
		Timestamp: time.Unix(3, 0),
	}
	exampleQuote = &v2.Quote{
		BidPrice:  4,
		BidSize:   5,
		AskPrice:  6,
		AskSize:   7,
		Timestamp: time.Unix(8, 0),
	}
)

func TestSnapshotWriterImpl_Write(t *testing.T) {
	t.Parallel()
	type fields struct {
		Timeframe string
		Timezone  *time.Location
	}
	tests := []struct {
		name              string
		fields            fields
		snapshots         map[string]*v2.Snapshot
		writeErr          error
		wantErr           bool
		wantTBKs          []io.TimeBucketKey
		wantCSMDataShapes []io.DataShape
		wantCSMLen        int
	}{
		{
			name: "OK/empty snapshot/snapshot with empty trade/quote is ignored",
			fields: fields{
				Timeframe: "1Sec",
				Timezone:  time.UTC,
			},
			snapshots: map[string]*v2.Snapshot{
				"AAPL": {
					LatestTrade: exampleTrade,
					LatestQuote: exampleQuote,
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
				{Name: "Ask", Type: io.FLOAT64},
				{Name: "AskSize", Type: io.UINT32},
				{Name: "Bid", Type: io.FLOAT64},
				{Name: "BidSize", Type: io.UINT32},
			},
			wantCSMLen: 1,
		},
		{
			name: "NG/failed to write to marketstore",
			fields: fields{
				Timeframe: "1Sec",
				Timezone:  time.UTC,
			},
			snapshots: map[string]*v2.Snapshot{
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

			q := SnapshotWriterImpl{
				MarketStoreWriter: msw,
				Timeframe:         tt.fields.Timeframe,
				Timezone:          tt.fields.Timezone,
			}
			err := q.Write(tt.snapshots)
			require.Equal(t, tt.wantErr, err != nil)

			tbks := msw.WrittenCSM.GetMetadataKeys()
			if tt.wantTBKs != nil {
				require.Equal(t, tt.wantTBKs, tbks)
			}

			if len(tt.wantCSMDataShapes) > 0 {
				require.Equal(t, tt.wantCSMDataShapes, msw.WrittenCSM[tbks[0]].GetDataShapes())
			}
		})
	}
}
