package feed_test

import (
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
)

var testBars = map[string][]alpaca.Bar{
	"AAPL": {
		{Time: time.Now().Add(-72 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 1},
		{Time: time.Now().Add(-48 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 2},
		{Time: time.Now().Add(-24 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 3},
	},
	"AMZN": {
		{Time: time.Now().Add(-72 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 4},
		{Time: time.Now().Add(-72 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 5},
		{Time: time.Now().Add(-72 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 6},
	},
	"FB": {
		{Time: time.Now().Add(-72 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 7},
		{Time: time.Now().Add(-48 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 8},
		{Time: time.Now().Add(-24 * time.Hour).Unix(), Open: 0, High: 0, Low: 0, Close: 0, Volume: 9},
	},
}

const errorSymbol = "ERROR"

type MockErrorAPIClient struct {
	testBars map[string][]alpaca.Bar
	internal.MockAPIClient
}

// ListBars returns an error if symbol:"ERROR" is included, but returns data to other symbols
func (mac *MockErrorAPIClient) ListBars(symbols []string, opts alpaca.ListBarParams) (map[string][]alpaca.Bar, error) {
	ret := make(map[string][]alpaca.Bar, 0)
	for _, symbl := range symbols {
		if symbl == errorSymbol {
			return nil, errors.New("error")
		}
		if bars, found := mac.testBars[symbl]; found {
			barPage := make([]alpaca.Bar, 0)

			// filter by time
			for _, bar := range bars {
				barTime := time.Unix(bar.Time, 0).UTC().Truncate(24 * time.Hour) // 00:00:00 of the bar time
				startDt := opts.StartDt.UTC().Truncate(24 * time.Hour)
				endDt := opts.EndDt.UTC().Truncate(24 * time.Hour)

				if barTime.Equal(startDt) || (barTime.After(startDt) && barTime.Before(startDt)) || barTime.Equal(endDt) {
					barPage = append(barPage, bar)
				}
			}
			//TODO: limit behavior
			ret[symbl] = barPage
		}
	}

	return ret, nil
}

type MockBarWriter struct {
	WriteCount int
}

func (mbw *MockBarWriter) Write(symbol string, bars []alpaca.Bar) error {
	// in order to assert the number of writen bars in the test
	mbw.WriteCount += len(bars)
	return nil
}

func TestBackfill_UpdateSymbols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		smbls               []string
		testBars            map[string][]alpaca.Bar
		barWriter           writer.BarWriter
		maxSymbolsPerReq    int
		maxBarsPerReq       int
		since               time.Time
		wantWrittenBarCount int
	}{
		{
			name:                "OK/All symbols are written",
			smbls:               []string{"AAPL", "AMZN", "FB"},
			testBars:            testBars,
			maxBarsPerReq:       2,
			maxSymbolsPerReq:    2,
			since:               time.Now().Add(-72 * time.Hour),
			wantWrittenBarCount: 9,
		},
		{
			name:                "OK/Pagination parameters don't affect total written count",
			smbls:               []string{"AAPL", "AMZN", "FB"},
			testBars:            testBars,
			maxBarsPerReq:       1,
			maxSymbolsPerReq:    3,
			since:               time.Now().Add(-72 * time.Hour),
			wantWrittenBarCount: 9,
		},
		{
			name:             "NG/Error page is not written",
			smbls:            []string{"AAPL", "AMZN", errorSymbol, "FB"},
			testBars:         testBars,
			maxBarsPerReq:    2,
			maxSymbolsPerReq: 2,
			since:            time.Now().Add(-72 * time.Hour),
			// firstPage=[AMZN, AAPL] so all data succeed.
			// secondPage=[error FB] so all data result in error.
			wantWrittenBarCount: 6,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var barWriter writer.BarWriter = &MockBarWriter{}

			symbolManager := internal.MockSymbolsManager{Symbols: tt.smbls}

			b := feed.NewBackfill(symbolManager,
				&MockErrorAPIClient{testBars: tt.testBars},
				barWriter,
				tt.since, tt.maxBarsPerReq, tt.maxSymbolsPerReq,
			)

			b.UpdateSymbols()

			if mbw, ok := barWriter.(*MockBarWriter); ok {
				assert.Equal(t, tt.wantWrittenBarCount, mbw.WriteCount)
			} else {
				assert.Fail(t, "[bug] type error")
			}
		})
	}
}
