package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/alpacahq/marketstore/contrib/calendar"
	"github.com/alpacahq/marketstore/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/log"
)

var (
	dir, from, to        string
	bars, quotes, trades bool
	parallelism          int
	apiKey               string

	// NY timezone
	NY, _  = time.LoadLocation("America/New_York")
	format = "2006-01-02"
)

func init() {
	flag.StringVar(&dir, "dir", "/project/data", "mktsdb directory to backfill to")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill from date (YYYY-MM-DD)")
	flag.BoolVar(&bars, "bars", false, "backfill bars")
	flag.BoolVar(&quotes, "quotes", false, "backfill quotes")
	flag.BoolVar(&trades, "trades", false, "backfill trades")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(), "parallelism (default NumCPU)")
	flag.StringVar(&apiKey, "apiKey", "", "polygon API key")

	flag.Parse()
}

func main() {
	initWriter()

	if apiKey == "" {
		log.Fatal("[polygon] api key is required")
	}

	api.SetAPIKey(apiKey)

	start, err := time.Parse(format, from)
	if err != nil {
		log.Fatal("[polygon] failed to parse from timestamp (%v)", err)
	}

	end, err := time.Parse(format, to)
	if err != nil {
		log.Fatal("[polygon] failed to parse to timestamp (%v)", err)
	}

	log.Info("[polygon] listing symbols")

	resp, err := api.ListSymbols()
	if err != nil {
		log.Fatal("[polygon] failed to list symbols (%v)", err)
	}

	sem := make(chan struct{}, parallelism)

	if bars {
		log.Info("[polygon] backfilling bars from %v to %v", start, end)

		for _, sym := range resp.Symbols {
			s := start
			e := end

			log.Info("[polygon] backfilling bars for %v", sym.Symbol)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err = backfill.Bars(sym.Symbol, t.Add(-24*time.Hour), t); err != nil {
							log.Warn("[polygon] failed to backfill trades for %v (%v)", sym.Symbol, err)
						}
					}(e)
				}
				e = e.Add(-24 * time.Hour)
			}
		}
	}

	if quotes {
		log.Info("[polygon] backfilling quotes from %v to %v", start, end)

		for _, sym := range resp.Symbols {
			s := start
			e := end

			log.Info("[polygon] backfilling quotes for %v", sym.Symbol)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err = backfill.Quotes(sym.Symbol, t.Add(-24*time.Hour), t); err != nil {
							log.Warn("[polygon] failed to backfill quotes for %v (%v)", sym.Symbol, err)
						}
					}(e)
				}
				e = e.Add(-24 * time.Hour)
			}
		}
	}

	if trades {
		log.Info("[polygon] backfilling trades from %v to %v", start, end)

		for _, sym := range resp.Symbols {
			s := start
			e := end

			log.Info("[polygon] backfilling trades for %v", sym.Symbol)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err = backfill.Trades(sym.Symbol, t.Add(-24*time.Hour), t); err != nil {
							log.Warn("[polygon] failed to backfill trades for %v (%v)", sym.Symbol, err)
						}
					}(e)
				}
				e = e.Add(-24 * time.Hour)
			}
		}
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	log.Info("[polygon] backfilling complete")
}

func initWriter() {
	utils.InstanceConfig.Timezone = NY
	utils.InstanceConfig.WALRotateInterval = 5

	executor.NewInstanceSetup(
		fmt.Sprintf("%v/mktsdb", dir),
		true, true, true, true)

	config := map[string]interface{}{
		"filter":       "nasdaq",
		"destinations": []string{"5Min", "15Min", "1H", "1D"},
	}

	trig, err := aggtrigger.NewTrigger(config)
	if err != nil {
		log.Fatal("[polygon] backfill failed to initialize writer (%v)", err)
	}

	executor.ThisInstance.TriggerMatchers = []*trigger.TriggerMatcher{
		trigger.NewMatcher(trig, "*/1Min/OHLCV"),
	}
}
