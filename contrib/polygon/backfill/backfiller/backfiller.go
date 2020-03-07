package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
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
	symbols              string
	parallelism          int
	apiKey               string
	exchanges            string

	// NY timezone
	NY, _  = time.LoadLocation("America/New_York")
	format = "2006-01-02"
)

func init() {
	flag.StringVar(&dir, "dir", "/project/data", "mktsdb directory to backfill to")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill from date (YYYY-MM-DD)")
	flag.StringVar(&exchanges, "exchanges", "*", "comma separated list of exchange")
	flag.BoolVar(&bars, "bars", false, "backfill bars")
	flag.BoolVar(&quotes, "quotes", false, "backfill quotes")
	flag.BoolVar(&trades, "trades", false, "backfill trades")
	flag.StringVar(&symbols, "symbols", "*",
		"comma separated list of symbols to backfill, the default * means backfill all symbols")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(), "parallelism (default NumCPU)")
	flag.StringVar(&apiKey, "apiKey", "", "polygon API key")

	flag.Parse()
}

func main() {
	// free memory in the background every 1 minute for long running
	// backfills with very high parallelism
	go func() {
		for {
			<-time.After(time.Minute)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memStart := m.Alloc
			log.Info("freeing memory...")
			debug.FreeOSMemory()
			runtime.ReadMemStats(&m)
			memEnd := m.Alloc
			log.Info(
				"mem stats: [start: %v end: %v freed: %v]",
				bytefmt.ByteSize(memStart),
				bytefmt.ByteSize(memEnd),
				bytefmt.ByteSize(memStart-memEnd),
			)
		}
	}()

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

	var symbolList []string
	if symbols == "*" {
		log.Info("[polygon] listing symbols")
		resp, err := api.ListTickers()
		if err != nil {
			log.Fatal("[polygon] failed to list symbols (%v)", err)
		}
		symbolList = make([]string, len(resp.Tickers))
		for i, s := range resp.Tickers {
			symbolList[i] = s.Ticker
		}
	} else {
		symbolList = strings.Split(symbols, ",")
	}

	var exchangeIDs []string
	if exchanges != "*" {
		exchangeIDs = strings.Split(exchanges, ",")
	}

	sem := make(chan struct{}, parallelism)

	if bars {
		log.Info("[polygon] backfilling bars from %v to %v", start, end)

		for _, sym := range symbolList {
			s := start
			e := end

			log.Info("[polygon] backfilling bars for %v", sym)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if len(exchangeIDs) == 0 {
							if err = backfill.Bars(sym, t.Add(-24*time.Hour), t); err != nil {
								log.Warn("[polygon] failed to backfill trades for %v (%v)", sym, err)
							}
						} else {
							if err = backfill.BuildBarsFromTrades(sym, t.Add(-24*time.Hour), t, exchangeIDs); err != nil {
								log.Warn("[polygon] failed to backfill trades for %v (%v)", sym, err)
							}
						}
					}(e)
				}
				e = e.Add(-24 * time.Hour)
			}
		}
	}

	if quotes {
		log.Info("[polygon] backfilling quotes from %v to %v", start, end)

		for _, sym := range symbolList {
			s := start
			e := end

			log.Info("[polygon] backfilling quotes for %v", sym)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err = backfill.Quotes(sym, t.Add(-24*time.Hour), t); err != nil {
							log.Warn("[polygon] failed to backfill quotes for %v (%v)", sym, err)
						}
					}(e)
				}
				e = e.Add(-24 * time.Hour)
			}
		}
	}

	if trades {
		log.Info("[polygon] backfilling trades from %v to %v", start, end)

		for _, sym := range symbolList {
			s := start
			e := end

			log.Info("[polygon] backfilling trades for %v", sym)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(e) {
					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err = backfill.Trades(sym, t.Add(-24*time.Hour), t); err != nil {
							log.Warn("[polygon] failed to backfill trades for %v (%v)", sym, err)
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
