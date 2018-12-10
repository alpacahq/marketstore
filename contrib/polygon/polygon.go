package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	nats "github.com/nats-io/go-nats"
)

type PolygonFetcher struct {
	config    FetcherConfig
	backfillM *sync.Map
	types     map[string]struct{}
}

type FetcherConfig struct {
	// polygon API key for authenticating with their APIs
	APIKey string `json:"api_key"`
	// polygon API base URL in case it is being proxied
	// (defaults to https://api.polygon.io/)
	BaseURL string `json:"base_url"`
	// list of nats servers to connect to
	// (defaults to "nats://nats1.polygon.io:30401, nats://nats2.polygon.io:30402, nats://nats3.polygon.io:30403")
	NatsServers string `json:"nats_servers"`
	// list of data types to subscribe to (one of bars, quotes, trades)
	DataTypes []string `json:"data_types"`
	// list of symbols that are important
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from the latest streamed bar
	QueryStart string `json:"query_start"`
}

const (
	Bars   = "bars"
	Quotes = "quotes"
	Trades = "trades"
)

var (
	minute = utils.NewTimeframe("1Min")
)

// NewBgWorker returns a new instances of PolygonFetcher. See FetcherConfig
// for more details about configuring PolygonFetcher.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	json.Unmarshal(data, &config)

	t := map[string]struct{}{}

	for _, dt := range config.DataTypes {
		if dt == Bars || dt == Quotes || dt == Trades {
			t[dt] = struct{}{}
		}
	}

	if len(t) == 0 {
		return nil, fmt.Errorf("at least one valid data_type is required")
	}

	return &PolygonFetcher{
		backfillM: &sync.Map{},
		config:    config,
		types:     t,
	}, nil
}

// Run the PolygonFetcher. It starts the streaming API as well as the
// asynchronous backfilling routine.
func (pf *PolygonFetcher) Run() {
	api.SetAPIKey(pf.config.APIKey)

	if pf.config.BaseURL != "" {
		api.SetBaseURL(pf.config.BaseURL)
	}

	if pf.config.NatsServers != "" {
		api.SetNatsServers(pf.config.NatsServers)
	}

	for t := range pf.types {
		go pf.stream(t)
	}

	select {}
}

func (pf *PolygonFetcher) stream(t string) {
	var err error

	log.Info("[polygon] streaming %v", t)

	switch t {
	case Bars:
		go pf.workBackfillBars()
		err = api.Stream(func(msg *nats.Msg) {
			handlers.Bar(msg, pf.backfillM)
		}, api.AggPrefix, pf.config.Symbols)
	case Quotes:
		err = api.Stream(handlers.Quote, api.QuotePrefix, pf.config.Symbols)
	case Trades:
		err = api.Stream(handlers.Trade, api.TradePrefix, pf.config.Symbols)
	}

	if err != nil {
		panic(fmt.Errorf("nats streaming error (%v)", err))
	}
}

func (pf *PolygonFetcher) workBackfillBars() {
	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {
		wg := sync.WaitGroup{}
		count := 0

		// range over symbols that need backfilling, and
		// backfill them from the last written record
		pf.backfillM.Range(func(key, value interface{}) bool {
			symbol := key.(string)
			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				go func() {
					wg.Add(1)
					defer wg.Done()

					// backfill the symbol in parallel
					pf.backfillBars(symbol, *value.(*int64))
					pf.backfillM.Store(key, nil)
				}()
			}

			// limit 10 goroutines per CPU core
			if count >= runtime.NumCPU()*10 {
				return false
			}

			return true
		})
		wg.Wait()
	}
}

func (pf *PolygonFetcher) backfillBars(symbol string, endEpoch int64) {
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
		q.SetEnd(endEpoch - int64(time.Minute.Seconds()))

		parsed, err := q.Parse()
		if err != nil {
			log.Error("[polygon] query parse failure (%v)", err)
			return
		}

		scanner, err := executor.NewReader(parsed)
		if err != nil {
			log.Error("[polygon] new scanner failure (%v)", err)
			return
		}

		csm, err := scanner.Read()
		if err != nil {
			log.Error("[polygon] scanner read failure (%v)", err)
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
	if err = backfill.Bars(symbol, from, time.Time{}); err != nil {
		log.Error("[polygon] bars backfill failure for key: [%v] (%v)", tbk.String(), err)
	}
}

func main() {}
