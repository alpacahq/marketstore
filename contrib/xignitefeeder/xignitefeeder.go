package main

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/feed"
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
	timeChecker := feed.NewDefaultMarketTimeChecker([]time.Time{}, true)

	return &feed.Worker{
		APIClient:          apiClient,
		MarketTimeChecker:  timeChecker,
		CSMWriter:          feed.MarketStoreWriter{},
		Timeframe:          config.Timeframe,
		Identifiers:        config.Identifiers,
		Interval:           config.Interval,
		LastExecutionTimes: map[string]time.Time{},
	}, nil
}

func main() {}
