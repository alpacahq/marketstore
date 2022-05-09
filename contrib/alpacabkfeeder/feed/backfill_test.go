package feed_test

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
)

var (
	dYear, dMonth, dDay    = time.Now().Add(-24 * time.Hour).Date()
	d                      = time.Date(dYear, dMonth, dDay, 0, 0, 0, 0, time.UTC)
	d2Year, d2Month, d2Day = time.Now().Add(-48 * time.Hour).Date()
	d2                     = time.Date(d2Year, d2Month, d2Day, 0, 0, 0, 0, time.UTC)
	d3Year, d3Month, d3Day = time.Now().Add(-72 * time.Hour).Date()
	d3                     = time.Date(d3Year, d3Month, d3Day, 0, 0, 0, 0, time.UTC)
)

var testBars = map[string][]api.Bar{
	"AAPL": {
		{Timestamp: d3, Open: 0, High: 0, Low: 0, Close: 0, Volume: 1},
		{Timestamp: d2, Open: 0, High: 0, Low: 0, Close: 0, Volume: 2},
		{Timestamp: d, Open: 0, High: 0, Low: 0, Close: 0, Volume: 3},
	},
	"AMZN": {
		{Timestamp: d3, Open: 0, High: 0, Low: 0, Close: 0, Volume: 4},
		{Timestamp: d2, Open: 0, High: 0, Low: 0, Close: 0, Volume: 5},
		{Timestamp: d, Open: 0, High: 0, Low: 0, Close: 0, Volume: 6},
	},
	"FB": {
		{Timestamp: d3, Open: 0, High: 0, Low: 0, Close: 0, Volume: 7},
		{Timestamp: d2, Open: 0, High: 0, Low: 0, Close: 0, Volume: 8},
		{Timestamp: d, Open: 0, High: 0, Low: 0, Close: 0, Volume: 9},
	},
}

const errorSymbol = "ERROR"

type MockErrorAPIClient struct {
	testBars map[string][]api.Bar
	internal.MockAPIClient
}

// GetMultiBars returns an error if symbol:"ERROR" is included, but returns data to other symbols.
func (mac *MockErrorAPIClient) GetMultiBars(symbols []string, opts api.GetBarsParams) (map[string][]api.Bar, error) {
	ret := make(map[string][]api.Bar)
	for _, symbl := range symbols {
		if symbl == errorSymbol {
			return nil, errors.New("error")
		}
		if bars, found := mac.testBars[symbl]; found {
			barPage := make([]api.Bar, 0)

			// filter by time
			for _, bar := range bars {
				barTime := time.Unix(bar.Timestamp.Unix(), 0).UTC().Truncate(24 * time.Hour) // 00:00:00 of the bar time
				startDt := opts.Start.UTC().Truncate(24 * time.Hour)
				endDt := opts.End.UTC().Truncate(24 * time.Hour)

				if barTime.Equal(startDt) || (barTime.After(startDt) && barTime.Before(startDt)) || barTime.Equal(endDt) {
					barPage = append(barPage, bar)
				}
			}
			// TODO: limit behavior
			ret[symbl] = barPage
		}
	}

	return ret, nil
}

type MockBarWriter struct {
	WriteCount int
}

func (mbw *MockBarWriter) Write(symbol string, bars []api.Bar) error {
	// in order to assert the number of written bars in the test
	mbw.WriteCount += len(bars)
	return nil
}

func TestBackfill_UpdateSymbols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		smbls               []string
		testBars            map[string][]api.Bar
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
