package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const requestError = "RequestError"

// Backfill aggregates daily chart data using Xignite API and store it to.
type Backfill struct {
	symbolManager symbols.Manager
	apiClient     api.Client
	writer        writer.QuotesWriter
	rangeWriter   writer.QuotesRangeWriter
	since         time.Time
}

// NewBackfill initializes the module to backfill the historical daily chart data to marketstore.
func NewBackfill(symbolManager symbols.Manager, apiClient api.Client, quotesWriter writer.QuotesWriter,
	rangeWriter writer.QuotesRangeWriter, since time.Time,
) *Backfill {
	return &Backfill{
		symbolManager: symbolManager, apiClient: apiClient,
		writer: quotesWriter, rangeWriter: rangeWriter, since: since,
	}
}

// Update calls UpdateSymbols and UpdateIndexSymbols functions sequentially.
func (b *Backfill) Update(ctx context.Context) {
	b.UpdateSymbols(ctx)
	b.UpdateIndexSymbols(ctx)
	// In order to get and store adjusted closing prices
	b.UpdateClosingPrice(ctx)
}

// UpdateSymbols aggregates daily chart data since the specified date
// and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore.
func (b *Backfill) UpdateSymbols(ctx context.Context) {
	endDate := time.Now().UTC()
	for _, identifier := range b.symbolManager.GetAllIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetQuotesRange(ctx, identifier, b.since, endDate)
		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == requestError {
				log.Info(fmt.Sprintf("failed to get the daily chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.rangeWriter.Write(resp.Security.Symbol, resp.ArrayOfEndOfDayQuote, false)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the daily chart data"+
				" to marketstore in UpdateSymbols. identifier=%v, err=%v", identifier, err))
		}

		log.Info("backfilling the historical daily chart data... identifier=%s", identifier)
	}

	log.Info("Data backfill is successfully done.")
}

// UpdateIndexSymbols aggregates daily chart data of index symbols
// since the specified date and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore.
func (b *Backfill) UpdateIndexSymbols(ctx context.Context) {
	endDate := time.Now().UTC()
	for _, identifier := range b.symbolManager.GetAllIndexIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetIndexQuotesRange(ctx, identifier, b.since, endDate)
		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == requestError {
				log.Info(fmt.Sprintf("failed to get the daily chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.rangeWriter.Write(resp.IndexAndGroup.Symbol, resp.ArrayOfEndOfDayQuote, true)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the daily chart data"+
				" to marketstore in UpdateIndexSymbols. identifier=%v, err=%v", identifier, err))
		}

		log.Info("backfilling the historical daily chart data... identifier=%s", identifier)
	}

	log.Info("Data backfill is successfully done.")
}

// UpdateClosingPrice get real-time quotes data for the target symbols and store them into the local marketstore server.
func (b *Backfill) UpdateClosingPrice(ctx context.Context) {
	// call Xignite API to get Quotes data
	identifiers := b.symbolManager.GetAllIdentifiers()
	response, err := b.apiClient.GetRealTimeQuotes(ctx, identifiers)
	if err != nil {
		log.Error(fmt.Sprintf("failed to get data from Xignite API. %v, err=", identifiers) + err.Error())
		return
	}

	// write Quotes data
	err = b.writer.Write(response)
	if err != nil {
		log.Error("failed to write quotes data. err=" + err.Error())
		return
	}

	log.Info("Storing adjusted closing prices has been successfully done.")
}
