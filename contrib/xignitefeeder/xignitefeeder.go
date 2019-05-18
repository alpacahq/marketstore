package main

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/feed"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/timer"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"time"
)

// NewBgWorker returns the new instance of XigniteFeeder.
// See configs.Config for the details of available configurations.
// nolint
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := configs.NewConfig(conf)

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to load config file. %v", conf))
	}
	log.Debug("loaded Xignite Feeder config...")

	// init Xignite API client
	apiClient := api.NewDefaultAPIClient(config.APIToken, config.Timeout)

	// init Market Time Checker
	timeChecker := feed.NewDefaultMarketTimeChecker(
		config.ClosedDaysOfTheWeek,
		config.ClosedDays,
		config.OpenTime,
		config.CloseTime)

	// init Symbols Manager to update symbols in the target exchanges every day
	sm := symbols.NewManager(apiClient, config.Exchanges)
	timer.RunEveryDayAt(config.UpdatingHour, sm.UpdateSymbols)

	// init QuotesRangeWriter to backfill daily chart data every day
	if config.Backfill.Enabled {
		msqrw := &writer.QuotesRangeWriterImpl{
			MarketStoreWriter: &writer.MarketStoreWriterImpl{},
			Timeframe:         config.Backfill.Timeframe,
		}

		bf := feed.NewBackfill(sm, apiClient, msqrw, time.Time(config.Backfill.Since))
		timer.RunEveryDayAt(config.UpdatingHour, bf.Update)
	}

	// init Quotes Writer
	var msqw writer.QuotesWriter = writer.QuotesWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Timeframe,
	}

	return &feed.Worker{
		MarketTimeChecker: timeChecker,
		APIClient:         apiClient,
		SymbolManager:     sm,
		QuotesWriter:      msqw,
		Interval:          config.Interval,
	}, nil
}

func main() {}
