package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/polygonconfig"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

type PolygonFetcher struct {
	config polygonconfig.FetcherConfig
	types  map[string]struct{} // Bars, Quotes, Trades
}

// NewBgWorker returns a new instances of PolygonFetcher. See FetcherConfig
// for more details about configuring PolygonFetcher.
// nolint:deadcode // plugin interface
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := json.Marshal(conf)
	config := polygonconfig.FetcherConfig{}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return
	}

	t := map[string]struct{}{}

	for _, dt := range config.DataTypes {
		if dt == "bars" || dt == "quotes" || dt == "trades" {
			t[dt] = struct{}{}
		}
	}

	if len(t) == 0 {
		return nil, fmt.Errorf("at least one valid data_type is required")
	}

	backfill.BackfillM = &sync.Map{}

	return &PolygonFetcher{
		config: config,
		types:  t,
	}, nil
}

// Run the PolygonFetcher. It starts the streaming API as well as the
// asynchronous backfilling routine.
func (pf *PolygonFetcher) Run() {
	api.SetAPIKey(pf.config.APIKey)

	if pf.config.BaseURL != "" {
		api.SetBaseURL(pf.config.BaseURL)
	}

	if pf.config.WSServers != "" {
		api.SetWSServers(pf.config.WSServers)
	}

	for t := range pf.types {
		var prefix api.Prefix
		var handler func([]byte)
		switch t {
		case "bars":
			prefix = api.Agg
			handler = handlers.BarsHandler
		case "quotes":
			prefix = api.Quote
			handler = handlers.QuoteHandler
		case "trades":
			prefix = api.Trade
			handler = handlers.TradeHandler
		}
		s := api.NewSubscription(prefix, pf.config.Symbols)
		s.Subscribe(handler)
	}

	select {}
}

func main() {}
