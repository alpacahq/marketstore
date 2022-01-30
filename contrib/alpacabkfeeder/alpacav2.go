package main

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/configs"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/timer"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// NewBgWorker returns the new instance of Alpaca Broker API Feeder.
// See configs.Config for the details of available configurations.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := configs.NewConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to load config file. %v", conf))
	}
	log.Info("loaded Alpaca Broker Feeder config...")

	// init Alpaca API client
	cred := &common.APIKey{
		ID:           config.APIKeyID,
		PolygonKeyID: config.APIKeyID,
		Secret:       config.APISecretKey,
		// OAuth:        os.Getenv(EnvApiOAuth),
	}
	if config.APIKeyID == "" || config.APISecretKey == "" {
		// if empty, get from env vars
		cred = common.Credentials()
	}
	apiClient := alpaca.NewClient(cred)

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
		log.Info(fmt.Sprintf("[Alpaca Broker Feeder] off_hours_schedule=%s[min] is set. "+
			"The data will be retrieved at %s [minute] even when the market is closed.",
			config.OffHoursSchedule, config.OffHoursSchedule),
		)
		timeChecker = feed.NewScheduledMarketTimeChecker(
			timeChecker,
			scheduleMin,
		)
	} else if config.OffHoursInterval != 0 {
		log.Info(fmt.Sprintf("[Alpaca Broker Feeder] off_hours_interval=%dmin is set. "+
			"The data will be retrieved every %d minutes even when the market is closed.",
			config.OffHoursInterval, config.OffHoursInterval),
		)
		timeChecker = feed.NewIntervalMarketTimeChecker(
			timeChecker,
			time.Duration(config.OffHoursInterval)*time.Minute,
		)
	}

	ctx := context.Background()
	// init Symbols Manager to update symbols in the target exchanges

	sm := symbols.NewManager(apiClient, config.Exchanges)
	sm.UpdateSymbols()
	timer.RunEveryDayAt(ctx, config.UpdateTime, sm.UpdateSymbols)
	log.Info("updated symbols in the target exchanges")

	// init SnapshotWriter
	var ssw writer.SnapshotWriter = writer.SnapshotWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Timeframe,
		Timezone:          utils.InstanceConfig.Timezone,
	}
	// init BarWriter
	var bw writer.BarWriter = writer.BarWriterImpl{
		MarketStoreWriter: &writer.MarketStoreWriterImpl{},
		Timeframe:         config.Backfill.Timeframe,
		Timezone:          utils.InstanceConfig.Timezone,
	}

	// init BarWriter to backfill daily chart data
	if config.Backfill.Enabled {
		const maxBarsPerRequest = 1000
		const maxSymbolsPerRequest = 100
		bf := feed.NewBackfill(sm, apiClient, bw, time.Time(config.Backfill.Since),
			maxBarsPerRequest, maxSymbolsPerRequest,
		)
		timer.RunEveryDayAt(ctx, config.UpdateTime, bf.UpdateSymbols)
	}

	return &feed.Worker{
		MarketTimeChecker: timeChecker,
		APIClient:         apiClient,
		SymbolManager:     sm,
		SnapshotWriter:    ssw,
		BarWriter:         bw,
		Interval:          config.Interval,
	}, nil
}

func main() {}
