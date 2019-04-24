package main

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/feed"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/timer"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"sync"
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
	timeChecker := feed.NewDefaultMarketTimeChecker(
		config.ClosedDaysOfTheWeek,
		configs.ToTimes(config.ClosedDays),
		time.Time(config.OpenTime),
		time.Time(config.CloseTime))

	// update symbols in the target exchanges every day
	sm := symbols.NewManager(apiClient, config.Exchanges)
	timer.RunEveryDayAt(config.UpdatingHour, sm.UpdateSymbols)

	// update daily chart data since 2008
	// timer.RunEveryDayAt(config.UpdatingHour, HistoricalDataBackFill)

	var msqw feed.QuotesWriter
	msqw = feed.MarketStoreQuotesWriter{LastExecutionTimes: sync.Map{}, Timeframe: config.Timeframe}

	return &feed.Worker{
		MarketTimeChecker: timeChecker,
		APIClient:         apiClient,
		SymbolManager:     sm,
		QuotesWriter:      msqw,
		Interval:          config.Interval,
	}, nil
}


func main() {}
