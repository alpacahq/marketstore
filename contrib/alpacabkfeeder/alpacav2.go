package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/configs"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/timer"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const getJSONFileTimeout = 10 * time.Second

// NewBgWorker returns the new instance of Alpaca Broker API Feeder.
// See configs.Config for the details of available configurations.
// nolint:deadcode // used as a plugin
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := configs.NewConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to load config file. %v", conf))
	}
	log.Info("loaded Alpaca Broker Feeder config...")

	apiCli := apiClient(config)

	// init Market Time Checker
	var timeChecker feed.MarketTimeChecker = defaultTimeChecker(config)
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

		if !config.ExtendedHours {
			log.Warn("[Alpaca Broker Feeder] both off_hours_schedule and extend_hours=false is set! " +
				"off-hour records won't be stored.")
		}
	}

	ctx := context.Background()
	// init symbols Manager to update symbols in the target exchanges
	var sm symbols.Manager
	sm = symbols.NewManager(apiCli, config.Exchanges)
	if config.StocksJSONURL != "" {
		// use a remote JSON file instead of the config.Exchanges to list up the symbols
		sm = symbols.NewJSONFileManager(&http.Client{Timeout: getJSONFileTimeout},
			config.StocksJSONURL, config.StocksJSONBasicAuth,
		)
		log.Info("updating symbols using a remote json file.")
	}
	sm.UpdateSymbols()
	if config.SymbolsUpdateTime.IsZero() {
		config.SymbolsUpdateTime = config.UpdateTime
	}
	timer.RunEveryDayAt(ctx, config.SymbolsUpdateTime, sm.UpdateSymbols)

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
		bf := feed.NewBackfill(sm, apiCli, bw, time.Time(config.Backfill.Since),
			maxBarsPerRequest, maxSymbolsPerRequest,
		)
		timer.RunEveryDayAt(ctx, config.UpdateTime, bf.UpdateSymbols)
	}

	return &feed.Worker{
		MarketTimeChecker: timeChecker,
		APIClient:         apiCli,
		SymbolManager:     sm,
		SnapshotWriter:    snapshotWriter(config),
		BarWriter:         bw,
		Interval:          config.Interval,
	}, nil
}

func apiClient(config *configs.DefaultConfig) *api.Client {
	// init Alpaca API client
	cred := &api.APIKey{
		ID:           config.APIKeyID,
		PolygonKeyID: config.APIKeyID,
		Secret:       config.APISecretKey,
		// OAuth:        os.Getenv(EnvApiOAuth),
		AuthMethod: api.AuthMethodFromString(config.AuthMethod),
	}
	if config.APIKeyID == "" || config.APISecretKey == "" {
		// if empty, get from env vars
		cred = api.Credentials()
	}
	return api.NewClient(cred)
}

func defaultTimeChecker(config *configs.DefaultConfig) *feed.DefaultMarketTimeChecker {
	return feed.NewDefaultMarketTimeChecker(
		config.ClosedDaysOfTheWeek,
		config.ClosedDays,
		config.OpenHourNY, config.OpenMinuteNY,
		config.CloseHourNY, config.CloseMinuteNY)
}

func snapshotWriter(config *configs.DefaultConfig) writer.SnapshotWriter {
	var tc writer.MarketTimeChecker = &writer.NoopMarketTimeChecker{}
	if !config.ExtendedHours {
		tc = defaultTimeChecker(config)
	}

	return writer.NewSnapshotWriterImpl(
		&writer.MarketStoreWriterImpl{},
		config.Timeframe,
		utils.InstanceConfig.Timezone,
		tc,
	)
}

func main() {}
