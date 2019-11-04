package main

import (
	"context"
	"fmt"
	"github.com/alpacahq/marketstore/utils"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/feed"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/timer"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
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

	ctx := context.Background()
	// init Symbols Manager to...
	// 1. update symbols in the target exchanges
	// 2. update index symbols in the target index groups
	// every day
	sm := symbols.NewManager(apiClient, config.Exchanges, config.IndexGroups)
	sm.Update()
	timer.RunEveryDayAt(ctx, config.UpdatingHour, sm.Update)

	// init QuotesRangeWriter to backfill daily chart data every day
	if config.Backfill.Enabled {
		msqrw := &writer.QuotesRangeWriterImpl{
			MarketStoreWriter: &writer.MarketStoreWriterImpl{},
			Timeframe:         config.Backfill.Timeframe,
		}

		bf := feed.NewBackfill(sm, apiClient, msqrw, time.Time(config.Backfill.Since))
		bf.Update()
		timer.RunEveryDayAt(ctx, config.UpdatingHour, bf.Update)
	}

	if config.RecentBackfill.Enabled {
		msbw := &writer.BarWriterImpl{
			MarketStoreWriter: &writer.MarketStoreWriterImpl{},
			Timeframe:         config.RecentBackfill.Timeframe,
			Timezone:          utils.InstanceConfig.Timezone,
		}
		rbf := feed.NewRecentBackfill(sm, timeChecker, apiClient, msbw, config.RecentBackfill.Days)
		rbf.Update()
		timer.RunEveryDayAt(ctx, config.UpdatingHour, rbf.Update)
	}

	// init Quotes Writer
	var msqw writer.QuotesWriter = writer.QuotesWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Timeframe,
		Timezone:          utils.InstanceConfig.Timezone,
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
