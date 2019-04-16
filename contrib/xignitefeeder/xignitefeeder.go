package main

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/feed"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"time"
)

// NewBgWorker returns the new instance of XigniteFeeder.  See feeder.Config
// for the details of available configurations.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := configs.NewConfig(conf)

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to load config file. %v", conf))
	}
	log.Debug("loaded Xignite Feeder config...")

	apiClient := api.NewDefaultAPIClient(config.APIToken, config.Timeout)
	// TODO : debug setting in mkts.yml
	timeChecker := feed.NewDefaultMarketTimeChecker(config.ClosedDaysOfTheWeek, configs.ToTimes(config.ClosedDays), time.Time(config.OpenTime), time.Time(config.CloseTime))
	sm := symbols.NewManager(apiClient, config.Exchanges)
	sm.UpdateEveryDayAt(config.UpdatingHour)

	return &feed.Worker{
		APIClient:          apiClient,
		MarketTimeChecker:  timeChecker,
		CSMWriter:          feed.MarketStoreWriter{},
		Timeframe:          config.Timeframe,
		Interval:           config.Interval,
		LastExecutionTimes: map[string]time.Time{},
		SymbolManager:      sm,
	}, nil
}

func main() {}
