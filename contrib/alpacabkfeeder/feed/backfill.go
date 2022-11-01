package feed

import (
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var backfillTimeframe = api.OneDay

// Backfill aggregates daily chart data using Alpava v2 API and store it to marketstore.
type Backfill struct {
	symbolManager    symbols.Manager
	apiClient        GetMultiBarsAPIClient
	barWriter        writer.BarWriter
	since            time.Time
	maxBarsPerReq    int
	maxSymbolsPerReq int
}

type GetMultiBarsAPIClient interface {
	GetMultiBars(symbols []string, params api.GetBarsParams) (map[string][]api.Bar, error)
}

// NewBackfill initializes the module to backfill the historical daily chart data to marketstore.
// Alpaca API spec: maxBarsPerRequest: 1000 bars per symbol per request at maximum
// Alpaca API spec: maxSymbolsPerRequest: 100 symbols per request at maximum.
func NewBackfill(symbolManager symbols.Manager, apiClient GetMultiBarsAPIClient, barWriter writer.BarWriter,
	since time.Time, maxBarsPerReq, maxSymbolsPerReq int,
) *Backfill {
	return &Backfill{
		symbolManager: symbolManager, apiClient: apiClient, barWriter: barWriter, since: since,
		maxBarsPerReq: maxBarsPerReq, maxSymbolsPerReq: maxSymbolsPerReq,
	}
}

// UpdateSymbols aggregates daily chart data since the specified date
// and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore.
func (b *Backfill) UpdateSymbols() {
	allSymbols := b.symbolManager.GetAllSymbols()
	y, m, d := time.Now().UTC().Date()
	until := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	// paginate symbols & paginate bars
	for idx := range pageIndex(len(allSymbols), b.maxSymbolsPerReq) {
		for dateRange := range datePageIndex(b.since, until, b.maxBarsPerReq) {
			// fmt.Printf("start=%v, end=%v, symbols=%v\n", dateRange.From, dateRange.To, allSymbols[idx.From:idx.To])
			params := api.GetBarsParams{
				TimeFrame: backfillTimeframe,
				Start:     time230000utc(dateRange.From),
				End:       maxPast16min(time230000utc(dateRange.To)),
				PageLimit: b.maxBarsPerReq,
			}

			// get data
			symbolBarsMap, err := b.apiClient.GetMultiBars(allSymbols[idx.From:idx.To], params)
			if err != nil {
				log.Error("Alpaca MarketData GetMultiBars API call error. params=%v, Err=%v", params, err)
				return
			}
			log.Info("Alpaca GetMultiBars API call: From=%v, To=%v, symbols=%v",
				dateRange.From, dateRange.To, allSymbols[idx.From:idx.To],
			)

			// write data
			for symbl, bars := range symbolBarsMap {
				err := b.barWriter.Write(symbl, bars)
				if err != nil {
					log.Error("failed to backfill the daily chart data "+
						"to marketstore in UpdateSymbols. symbol=%v, err=%v", symbl, err)
				}
			}
		}
	}

	log.Info("[Alpaca Broker Feeder] daily chart backfill is successfully done.")
}

// Alpaca GetMultiBars API returns daily chart data based on US time.
// e.g. When 1D bar is requested with time.Date(2021, 12,1,0,0,0,0,time.UTC),
// the API returns a daily chart for 2021-11-30 because 2021-12-01 00:00:00 UTC is 2021-11-30 19:00:00 EST.
// So it's safe to always provide yyyy-mm-dd 23:00:00 UTC to the API when daily chart is necessary
// because it can be considered that the market for the day is already closed at 23:00:00 UTC
// regardless of the US timezones (EST, EDT).
func time230000utc(time2 time.Time) time.Time {
	y, m, d := time2.Date()
	t := time.Date(y, m, d, 23, 0, 0, 0, time.UTC)
	return t
}

// Alpaca API doesn't allow querying historical bars data from the past 15 minutes depending on the subscription.
// https://alpaca.markets/docs/market-data/#subscription-plans
// maxPast16min returns the specified time or the time 16 minutes ago from now,
// to avoid "your subscription does not permit querying data from the past 15 minutes" error.
func maxPast16min(time2 time.Time) time.Time {
	past16min := time.Now().Add(-16 * time.Minute)
	if time2.After(past16min) {
		return past16min
	}
	return time2
}

// utilities for pagination.
type index struct {
	From, To int
}

func pageIndex(length, pageSize int) <-chan index {
	ch := make(chan index)

	go func() {
		defer close(ch)

		for i := 0; i < length; i += pageSize {
			idx := index{i, i + pageSize}
			if length < idx.To {
				idx.To = length
			}
			ch <- idx
		}
	}()

	return ch
}

type dateRange struct {
	From, To time.Time
}

// datePageIndex returns a channel with paginated date ranges.
// datePageIndex assumes that start and end have only year, month, and day information
// like time.Date(yyyy, mm, dd, 0,0,0,0, time.UTC)
// e.g. start = 2021-12-01, end = 2021-12-06, pageDays = 2
// -> chan will return
// [
//	{From:2021-12-01, To:2021-12-03},
//	{From:2021-12-03, To:2021-12-05},
//	{From:2021-12-05, To:2021-12-06}
// ].
func datePageIndex(start, end time.Time, pageDays int) <-chan dateRange {
	ch := make(chan dateRange)

	go func() {
		defer close(ch)

		i := start
		for {
			pageStart := i
			pageEnd := i.AddDate(0, 0, pageDays)
			if pageEnd.After(end) {
				pageEnd = end
			}
			page := dateRange{From: pageStart, To: pageEnd}
			ch <- page

			i = i.AddDate(0, 0, pageDays)
			if i.Equal(end) || i.After(end) {
				break
			}
		}
	}()

	return ch
}
