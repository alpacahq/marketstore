package feed

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const backfillTimeframe = "1D"

// Backfill aggregates daily chart data using Alpava v2 API and store it to marketstore
type Backfill struct {
	symbolManager    symbols.Manager
	apiClient        ListBarsAPIClient
	barWriter        writer.BarWriter
	since            time.Time
	maxBarsPerReq    int
	maxSymbolsPerReq int
}

type ListBarsAPIClient interface {
	ListBars(symbols []string, opts alpaca.ListBarParams) (map[string][]alpaca.Bar, error)
}

// NewBackfill initializes the module to backfill the historical daily chart data to marketstore.
// Alpaca API spec: maxBarsPerRequest: 1000 bars per symbol per request at maximum
// Alpaca API spec: maxSymbolsPerRequest: 100 symbols per request at maximum
func NewBackfill(symbolManager symbols.Manager, apiClient ListBarsAPIClient, barWriter writer.BarWriter, Since time.Time,
	maxBarsPerReq, maxSymbolsPerReq int,
) *Backfill {
	return &Backfill{symbolManager: symbolManager, apiClient: apiClient, barWriter: barWriter, since: Since,
		maxBarsPerReq: maxBarsPerReq, maxSymbolsPerReq: maxSymbolsPerReq,
	}
}

// UpdateSymbols aggregates daily chart data since the specified date and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore
func (b *Backfill) UpdateSymbols() {
	allSymbols := b.symbolManager.GetAllSymbols()

	// paginate symbols & paginate bars
	for idx := range pageIndex(len(allSymbols), b.maxSymbolsPerReq) {
		for dateRange := range datePageIndex(b.since, time.Now().UTC(), b.maxBarsPerReq) {
			// fmt.Printf("start=%v, end=%v, symbols=%v\n", dateRange.From, dateRange.To, allSymbols[idx.From:idx.To])
			params := alpaca.ListBarParams{
				Timeframe: backfillTimeframe,
				StartDt:   &dateRange.From,
				EndDt:     &dateRange.To,
				Limit:     &b.maxBarsPerReq,
			}

			// get data
			symbolBarsMap, err := b.apiClient.ListBars(allSymbols[idx.From:idx.To], params)
			if err != nil {
				log.Error("Alpaca Broker ListBars API call error. Err=%v", err)
				return
			}
			log.Info("Alpaca ListBars API call: From=%v, To=%v, Symbols=%v", dateRange.From, dateRange.To, allSymbols[idx.From:idx.To])

			// write data
			for symbl, bars := range symbolBarsMap {
				err := b.barWriter.Write(symbl, bars)
				if err != nil {
					log.Error(fmt.Sprintf("failed to backfill the daily chart data to marketstore in UpdateSymbols. symbol=%v, err=%v", symbl, err))
				}
			}
		}
	}

	log.Info("[Alpaca Broker Feeder] daily chart backfill is successfully done.")
}

// utilities for pagination
type index struct {
	From, To int
}

func pageIndex(length int, pageSize int) <-chan index {
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

func datePageIndex(start, end time.Time, pageDays int) <-chan dateRange {
	ch := make(chan dateRange)

	go func() {
		defer close(ch)
		startDayBegin := start.Round(24 * time.Hour)
		endDayBegin := end.Round(24 * time.Hour)
		for i := startDayBegin; endDayBegin.Unix() >= i.Unix(); i = i.AddDate(0, 0, pageDays) {
			idx := dateRange{i, i.AddDate(0, 0, pageDays)}

			if idx.To.After(endDayBegin) {
				idx.To = endDayBegin
			}
			ch <- idx
		}
	}()

	return ch
}
