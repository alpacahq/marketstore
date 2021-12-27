package feed

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// RecentBackfill aggregates daily chart data using Xignite API and store it to.
type RecentBackfill struct {
	symbolManager     symbols.Manager
	marketTimeChecker MarketTimeChecker
	apiClient         api.Client
	writer            writer.BarWriter
	days              int
}

// NewRecentBackfill initializes the module to backfill the historical 5-minute chart data to marketstore.
func NewRecentBackfill(sm symbols.Manager, mtc MarketTimeChecker, ac api.Client, writer writer.BarWriter, days int,
) *RecentBackfill {
	return &RecentBackfill{symbolManager: sm, marketTimeChecker: mtc, apiClient: ac, writer: writer, days: days}
}

func (b *RecentBackfill) Update() {
	b.UpdateSymbols()
	b.UpdateIndexSymbols()
}

// UpdateSymbols aggregates recent chart data for the past X business days and store it to "{symbol}/5Min/OHLCV" bucket in marketstore.
func (b *RecentBackfill) UpdateSymbols() {
	endDate := time.Now().UTC()
	// get the date of {b.days} business days ago
	startDate, err := b.marketTimeChecker.Sub(endDate, b.days)
	if err != nil {
		log.Error("startDate of the recent backfill should be a past date. RecentBackfill.days=" + fmt.Sprint(b.days))
		return
	}

	for _, identifier := range b.symbolManager.GetAllIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetRealTimeBars(identifier, startDate, endDate)
		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("failed to get the recent chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.writer.Write(resp.Security.Symbol, resp.ArrayOfBar, false)
		if err != nil {
			log.Error(fmt.Sprintf("failed to backfill the recent chart data to marketstore. identifier=%v. Err=%v", identifier, err))
		}

		log.Info("backfilling the recent chart data... identifier=%s", identifier)
	}

	log.Info("Recent Data backfill has successfully been done.")
}

// UpdateIndexSymbols aggregates recent chart data of index symbols for the past X business days and store it to "{symbol}/5Min/OHLCV" bucket in marketstore.
func (b *RecentBackfill) UpdateIndexSymbols() {
	endDate := time.Now().UTC()
	// get the date of {b.days} business days ago
	startDate, err := b.marketTimeChecker.Sub(endDate, b.days)
	if err != nil {
		log.Error("(index symbols) startDate of the recent backfill should be a past date. RecentBackfill.days=" + fmt.Sprint(b.days))
		return
	}

	for _, identifier := range b.symbolManager.GetAllIndexIdentifiers() {
		// call a Xignite API to get the historical data
		resp, err := b.apiClient.GetIndexBars(identifier, startDate, endDate)
		if err != nil {
			// The RequestError is returned when the symbol doesn't have any quotes data
			// (i.e. the symbol has not been listed yet)
			if resp.Outcome == "RequestError" {
				log.Info(fmt.Sprintf("(index symbols) failed to get the recent chart data for identifier=%s. Err=%v", identifier, err))
				continue
			}
			log.Error("(index symbols) Xignite API call error. Err=%v, API response=%v", err, resp)
			return
		}

		// write the data to marketstore
		err = b.writer.Write(resp.IndexAndGroup.Symbol, resp.ArrayOfBar, true)
		if err != nil {
			log.Error(fmt.Sprintf("(index symbols) failed to backfill the recent chart data to marketstore. identifier=%v. Err=%v", identifier, err))
		}

		log.Info("(index symbols) backfilling the recent chart data... identifier=%s", identifier)
	}

	log.Info("Recent Data backfill for index symbols has successfully been done.")
}
