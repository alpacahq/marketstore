package feed

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/utils/log"
)

// GranularBackfill aggregates daily chart data using Xignite API and store it to
type GranularBackfill struct {
	symbolManager     symbols.Manager
	marketTimeChecker MarketTimeChecker
	apiClient         api.Client
	writer            writer.BarWriter
	days              int
}

// NewBackfill initializes the module to backfill the historical 5-minute chart data to marketstore
func NewGranularBackfill(sm symbols.Manager, mtc MarketTimeChecker, ac api.Client, writer writer.BarWriter, days int,
) *GranularBackfill {
	return &GranularBackfill{symbolManager: sm, marketTimeChecker: mtc, apiClient: ac, writer: writer, days: days}
}

// Update aggregates granular chart data for the past X business days and store it to "{symbol}/5Min/OHLCV" bucket in marketstore
func (b *GranularBackfill) Update() {
	endDate := time.Now().UTC()
	// get the date of {b.days} business days ago
	startDate, err := b.marketTimeChecker.Sub(endDate, b.days)
	if err != nil {
		log.Error("startDate of the granular backfill should be a past date. GranularBackfill.days=" + string(b.days))
		return
	}

	for _, identifier := range b.symbolManager.GetAllIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetRealTimeBars(identifier, startDate, endDate)

		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("failed to get the granular chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.writer.Write(resp)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the granular chart data to marketstore. identifier=%v. Err=%v", identifier, err))
		}

		log.Info("backfilling the granular chart data... identifier=%s", identifier)
	}

	log.Info("Granular Data backfill has successfully been done.")
}
