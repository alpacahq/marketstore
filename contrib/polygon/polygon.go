package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/polygon_config"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const defaultBatchSize = 50000

type PolygonFetcher struct {
	config polygon_config.FetcherConfig
	types  map[string]struct{} // Bars, Quotes, Trades
}

// NewBgWorker returns a new instances of PolygonFetcher. See FetcherConfig
// for more details about configuring PolygonFetcher.
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := json.Marshal(conf)
	config := polygon_config.FetcherConfig{}
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

func (pf *PolygonFetcher) backfillBars(client *http.Client, symbol string, end time.Time, writerWP *worker.WorkerPool) {
	var (
		from time.Time
		err  error
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	)

	// query the latest entry prior to the streamed record
	if pf.config.QueryStart == "" {
		instance := executor.ThisInstance
		cDir := instance.CatalogDir
		q := planner.NewQuery(cDir)
		q.AddTargetKey(tbk)
		q.SetRowLimit(io.LAST, 1)
		q.SetEnd(end.Add(-1 * time.Minute))

		parsed, err2 := q.Parse()
		if err2 != nil {
			log.Error("[polygon] query parse failure (%v)", err2)
			return
		}

		scanner, err2 := executor.NewReader(parsed)
		if err2 != nil {
			log.Error("[polygon] new scanner failure (%v)", err2)
			return
		}

		csm, err2 := scanner.Read()
		if err2 != nil {
			log.Error("[polygon] scanner read failure (%v)", err2)
			return
		}

		epoch := csm[*tbk].GetEpoch()

		// no gap to fill
		if len(epoch) == 0 {
			return
		}

		from = time.Unix(epoch[len(epoch)-1], 0)
	} else {
		for _, layout := range []string{
			"2006-01-02 03:04:05",
			"2006-01-02T03:04:05",
			"2006-01-02 03:04",
			"2006-01-02T03:04",
			"2006-01-02",
		} {
			from, err = time.Parse(layout, pf.config.QueryStart)
			if err == nil {
				break
			}
		}
	}

	// request & write the missing bars
	err = backfill.Bars(client, symbol, from, time.Time{}, defaultBatchSize, false, writerWP)
	if err != nil {
		log.Error("[polygon] bars backfill failure for key: [%v] (%v)", tbk.String(), err)
	}
}

func main() {}
