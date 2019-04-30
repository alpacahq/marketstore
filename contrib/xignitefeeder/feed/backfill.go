package feed

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/utils/log"
	"time"
)

//Backfill aggregates daily chart data using Xignite API and store it to
type Backfill struct {
	symbolManager *symbols.Manager
	apiClient     api.Client
	writer        writer.QuotesRangeWriter
	since         time.Time
}

func NewBackfill(symbolManager *symbols.Manager, apiClient api.Client, writer writer.QuotesRangeWriter, Since time.Time) *Backfill {
	return &Backfill{symbolManager: symbolManager, apiClient: apiClient, writer: writer, since: Since}
}

// Update aggregates daily chart data since the specified date and store it to "{symbol}/{timeframe}/OHLCV" bucket in marketstore
func (b *Backfill) Update() {
	endDate := time.Now().UTC()
	for _, identifier := range b.symbolManager.GetAllIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetQuotesRange(identifier, b.since, endDate)

		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("failed to get quotes data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.writer.Write(resp)
		if err != nil {
			log.Error(fmt.Sprintf("failed to write QuotesRange data to marketstore. identifier=%v", identifier))
		}

		log.Debug("backfilling the historical daily chart data... identifier=%s", identifier)
	}

	log.Debug("Data backfill is successfully done.")
}
