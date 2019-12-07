package feed

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/utils/log"
)

// Backfill aggregates daily chart data using Xignite API and store it to
type Backfill struct {
	symbolManager symbols.Manager
	apiClient     api.Client
	writer        writer.QuotesWriter
	rangeWriter   writer.QuotesRangeWriter
	since         time.Time
}

// NewBackfill initializes the module to backfill the historical daily chart data to marketstore
func NewBackfill(symbolManager symbols.Manager, apiClient api.Client, writer writer.QuotesWriter,
	rangeWriter writer.QuotesRangeWriter, Since time.Time,
) *Backfill {
	return &Backfill{symbolManager: symbolManager, apiClient: apiClient,
		writer: writer, rangeWriter: rangeWriter, since: Since,
	}
}

// Update calls UpdateSymbols and UpdateIndexSymbols functions sequentially
func (b *Backfill) Update() {
	b.UpdateSymbols()
	b.UpdateIndexSymbols()
	// In order to get and store adjusted closing prices
	b.UpdateClosingPrice()
}

// UpdateSymbols aggregates daily chart data since the specified date and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore
func (b *Backfill) UpdateSymbols() {
	endDate := time.Now().UTC()
	for _, identifier := range b.symbolManager.GetAllIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetQuotesRange(identifier, b.since, endDate)

		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("failed to get the daily chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.rangeWriter.Write(resp.Security.Symbol, resp.ArrayOfEndOfDayQuote, false)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the daily chart data to marketstore. identifier=%v", identifier))
		}

		log.Info("backfilling the historical daily chart data... identifier=%s", identifier)
	}

	log.Info("Data backfill is successfully done.")
}

// UpdateIndexSymbols aggregates daily chart data of index symbols
// since the specified date and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore
func (b *Backfill) UpdateIndexSymbols() {
	endDate := time.Now().UTC()
	for _, identifier := range b.symbolManager.GetAllIndexIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetIndexQuotesRange(identifier, b.since, endDate)

		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("failed to get the daily chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.rangeWriter.Write(resp.IndexAndGroup.Symbol, resp.ArrayOfEndOfDayQuote, true)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the daily chart data to marketstore. identifier=%v", identifier))
		}

		log.Info("backfilling the historical daily chart data... identifier=%s", identifier)
	}

	log.Info("Data backfill is successfully done.")
}

// UpdateClosingPrice get real-time quotes data for the target symbols and store them into the local marketstore server.
func (b *Backfill) UpdateClosingPrice() {
	// call Xignite API to get Quotes data
	identifiers := b.symbolManager.GetAllIdentifiers()
	response, err := b.apiClient.GetRealTimeQuotes(identifiers)
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
