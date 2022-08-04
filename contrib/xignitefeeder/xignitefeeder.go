package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/feed"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/timer"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// NewBgWorker returns the new instance of XigniteFeeder.
// See configs.Config for the details of available configurations.
// nolint:deadcode // used by plugin
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := configs.NewConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to load config file. %v", conf))
	}
	log.Info("loaded Xignite Feeder config...")

	// init Xignite API client
	apiClient := api.NewDefaultAPIClient(config.APIToken, config.Timeout)

	// init Market Time Checker
	var timeChecker feed.MarketTimeChecker
	timeChecker = feed.NewDefaultMarketTimeChecker(
		config.ClosedDaysOfTheWeek,
		config.ClosedDays,
		config.OpenTime,
		config.CloseTime)
	if config.OffHoursSchedule != "" {
		scheduleMin, err := feed.ParseSchedule(config.OffHoursSchedule)
		if err != nil {
			return nil, fmt.Errorf("parse off_hours_schedule %s: %w", config.OffHoursSchedule, err)
		}
		log.Info(fmt.Sprintf("[Xignite Feeder] off_hours_schedule=%s[min] is set. "+
			"The data will be retrieved at %s [minute] even when the market is closed.",
			config.OffHoursSchedule, config.OffHoursSchedule),
		)
		timeChecker = feed.NewScheduledMarketTimeChecker(
			timeChecker,
			scheduleMin,
		)
	}

	ctx := context.Background()
	// init symbols Manager to...
	// 1. update symbols in the target exchanges
	// 2. update index symbols in the target index groups
	// every day
	sm := symbols.NewManager(apiClient, config.Exchanges, config.IndexGroups, config.NotQuoteStockList)
	sm.Update(ctx)
	timer.RunEveryDayAt(ctx, config.UpdateTime, sm.Update)
	log.Info("updated symbols in the target exchanges")

	// init Quotes Writer & QuotesRange Writer
	var msqw writer.QuotesWriter = writer.QuotesWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Timeframe,
		Timezone:          utils.InstanceConfig.Timezone,
	}
	var msqrw writer.QuotesRangeWriter = &writer.QuotesRangeWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Backfill.Timeframe,
	}

	// init QuotesRangeWriter to backfill daily chart data every day
	if config.Backfill.Enabled {
		bf := feed.NewBackfill(sm, apiClient, msqw, msqrw, time.Time(config.Backfill.Since))
		timer.RunEveryDayAt(ctx, config.UpdateTime, bf.Update)
		log.Info("backfilled daily chart in the target exchanges")
	}

	if config.RecentBackfill.Enabled {
		msbw := &writer.BarWriterImpl{
			MarketStoreWriter: &writer.MarketStoreWriterImpl{},
			Timeframe:         config.RecentBackfill.Timeframe,
			Timezone:          utils.InstanceConfig.Timezone,
		}
		rbf := feed.NewRecentBackfill(sm, timeChecker, apiClient, msbw, config.RecentBackfill.Days)
		timer.RunEveryDayAt(ctx, config.UpdateTime, rbf.Update)
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
