package main

import (
	"flag"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"
	"github.com/gobwas/glob"

	"code.cloudfoundry.org/bytefmt"
	"github.com/alpacahq/marketstore/v4/cmd/start"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	dir, from, to                       string
	barPeriod, tradePeriod, quotePeriod string
	bars, quotes, trades                bool
	symbols                             string
	parallelism                         int
	apiKey                              string
	exchanges                           string
	batchSize                           int
	cacheDir                            string
	readFromCache                       bool
	noIngest                            bool
	unadjusted                          bool
	// NY timezone
	NY, _          = time.LoadLocation("America/New_York")
	configFilePath string

	format = "2006-01-02"
)

func init() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory to backfill to. If empty, the dir is taken from mkts.yml")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format), "backfill from date (YYYY-MM-DD) [included]")
	flag.StringVar(&to, "to", time.Now().Format(format), "backfill to date (YYYY-MM-DD) [not included]")
	flag.StringVar(&exchanges, "exchanges", "*", "comma separated list of exchange")
	flag.BoolVar(&bars, "bars", false, "backfill bars")
	flag.StringVar(&barPeriod, "bar-period", (60 * 24 * time.Hour).String(), "backfill bar period")
	flag.StringVar(&tradePeriod, "trade-period", (10 * 24 * time.Hour).String(), "backfill trade period")
	flag.StringVar(&quotePeriod, "quote-period", (10 * 24 * time.Hour).String(), "backfill quote period")
	flag.BoolVar(&quotes, "quotes", false, "backfill quotes")
	flag.BoolVar(&trades, "trades", false, "backfill trades")
	flag.StringVar(&symbols, "symbols", "*",
		"glob pattern of symbols to backfill, the default * means backfill all symbols")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(), "parallelism (default NumCPU)")
	flag.IntVar(&batchSize, "batchSize", 50000, "batch/pagination size for downloading trades, quotes, & bars")
	flag.StringVar(&apiKey, "apiKey", "", "polygon API key")
	flag.StringVar(&cacheDir, "cache-dir", "", "directory to dump polygon's json replies")
	flag.BoolVar(&readFromCache, "read-from-cache", false, "read cached results if available")
	flag.BoolVar(&noIngest, "no-ingest", false, "do not ingest downloaded data, just store it in cache")
	flag.BoolVar(&unadjusted, "unadjusted", false, "request unadjusted price data")
	flag.StringVar(&configFilePath, "config", "/etc/mkts.yml", "path to the mkts.yml config file")

	flag.Parse()
}

func main() {
	rootDir, triggers, walRotateInterval := initConfig()
	_, shutdownPending, walWG := initWriter(rootDir, triggers, walRotateInterval)

	// free memory in the background every 1 minute for long running backfills with very high parallelism
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

	if apiKey == "" {
		log.Fatal("[polygon] api key is required")
	}

	if noIngest && cacheDir == "" {
		log.Fatal("[polygon] no-ingest should only be specified when cache-dir is set")
	}
	backfill.NoIngest = noIngest

	api.SetAPIKey(apiKey)

	start, err := time.Parse(format, from)
	if err != nil {
		log.Fatal("[polygon] failed to parse from timestamp (%v)", err)
	}

	end, err := time.Parse(format, to)
	if err != nil {
		log.Fatal("[polygon] failed to parse to timestamp (%v)", err)
	}

	tradePeriodDuration, err := parseAndValidateDuration(tradePeriod, 60*24*time.Hour, 24*time.Hour)
	if err != nil {
		log.Fatal("[polygon] failed to parse trade-period duration (%v)", err)
	}

	quotePeriodDuration, err := parseAndValidateDuration(quotePeriod, 60*24*time.Hour, 24*time.Hour)
	if err != nil {
		log.Fatal("[polygon] failed to parse trade-period duration (%v)", err)
	}

	barPeriodDuration, err := parseAndValidateDuration(barPeriod, 60*24*time.Hour, 24*time.Hour)
	if err != nil {
		log.Fatal("[polygon] failed to parse trade-period duration (%v)", err)
	}

	if cacheDir != "" {
		err = os.MkdirAll(cacheDir, 0777)
		if err != nil {
			log.Fatal("[polygon] cannot create json dump directory (%v)", err)
		}
		log.Info("[polygon] using %s to dump polygon's replies", cacheDir)
		api.CacheDir = cacheDir
		api.FromCache = readFromCache
	}

	startTime := time.Now()

	log.Info("[polygon] listing symbols for pattern: %v", symbols)
	pattern := glob.MustCompile(symbols)
	symbolList := make([]string, 0)
	symbolListMux := new(sync.Mutex)
	tickerListRunning := true
	tickerListWP := worker.NewWorkerPool(parallelism)

	for page := 0; tickerListRunning; page++ {
		currentPage := page

		tickerListWP.Do(func() {
			getTicker(currentPage, pattern, &symbolList, symbolListMux, &tickerListRunning)
		})
	}

	tickerListWP.CloseAndWait()
	symbolList = unique(symbolList)
	sort.Strings(symbolList)
	if len(symbolList) == 0 {
		log.Fatal("no symbol selected")
	}
	log.Info("[polygon] selected %v symbols", len(symbolList))

	var exchangeIDs []int
	if exchanges != "*" {
		for _, exchangeIDStr := range strings.Split(exchanges, ",") {
			exchangeIDInt, err := strconv.Atoi(exchangeIDStr)
			if err != nil {
				log.Fatal("Invalid exchange ID: %v", exchangeIDStr)
			}

			exchangeIDs = append(exchangeIDs, exchangeIDInt)
		}
	}

	if bars {
		apiCallerWP := worker.NewWorkerPool(parallelism)
		writerWP := worker.NewWorkerPool(1)
		log.Info("[polygon] backfilling bars from %v to %v", start, end)

		for _, sym := range symbolList {
			currentSymbol := sym
			apiCallerWP.Do(func() {
				getBars(start, end, barPeriodDuration, currentSymbol, exchangeIDs, unadjusted, writerWP)
			})
		}

		log.Info("[polygon] wait for api workers")
		apiCallerWP.CloseAndWait()
		log.Info("[polygon] wait for writer workers")
		writerWP.CloseAndWait()
	}

	if quotes {
		apiCallerWP := worker.NewWorkerPool(parallelism)
		writerWP := worker.NewWorkerPool(1)
		log.Info("[polygon] backfilling quotes from %v to %v", start, end)

		for _, sym := range symbolList {
			currentSymbol := sym
			apiCallerWP.Do(func() {
				getQuotes(start, end, quotePeriodDuration, currentSymbol, writerWP)
			})
		}

		log.Info("[polygon] wait for api workers")
		apiCallerWP.CloseAndWait()
		log.Info("[polygon] wait for writer workers")
		writerWP.CloseAndWait()

	}

	if trades {
		apiCallerWP := worker.NewWorkerPool(parallelism)
		writerWP := worker.NewWorkerPool(1)
		log.Info("[polygon] backfilling trades from %v to %v", start, end)

		for _, sym := range symbolList {
			currentSymbol := sym
			apiCallerWP.Do(func() {
				getTrades(start, end, tradePeriodDuration, currentSymbol, writerWP)
			})
		}

		log.Info("[polygon] wait for api workers")
		apiCallerWP.CloseAndWait()
		log.Info("[polygon] wait for writer workers")
		writerWP.CloseAndWait()
	}

	log.Info("[polygon] wait for shutdown")
	if shutdownPending != nil {
		*shutdownPending = true
	}
	walWG.Wait()
	executor.FinishAndWait()

	log.Info("[polygon] api call time %s", backfill.ApiCallTime)
	log.Info("[polygon] wait time %s", backfill.WaitTime)
	log.Info("[polygon] write time %s", backfill.WriteTime)
	log.Info("[polygon] backfilling complete %s", time.Now().Sub(startTime))
}

func initConfig() (rootDir string, triggers []*utils.TriggerSetting, walRotateInterval int) {
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatal("failed to read configuration file error: %s", err.Error())
		os.Exit(1)
	}

	config, err := utils.InstanceConfig.Parse(data)
	if err != nil {
		log.Fatal("failed to parse configuration file error: %v", err.Error())
		os.Exit(1)
	}

	return config.RootDirectory, config.Triggers, config.WALRotateInterval
}

func initWriter(rootDir string, triggers []*utils.TriggerSetting, walRotateInterval int,
) (instanceConfig *executor.InstanceMetadata, shutdownPending *bool, walWG *sync.WaitGroup) {
	instanceConfig, shutdownPending, walWG = executor.NewInstanceSetup(rootDir, nil, walRotateInterval, true, true, true, true)
	// if configured, also load the ondiskagg triggers
	for _, triggerSetting := range triggers {
		if triggerSetting.Module == "ondiskagg.so" {
			tmatcher := start.NewTriggerMatcher(triggerSetting)
			executor.ThisInstance.TriggerMatchers = append(instanceConfig.TriggerMatchers, tmatcher)
		}
	}
	return instanceConfig, shutdownPending, walWG
}

func getTicker(page int, pattern glob.Glob, symbolList *[]string, symbolListMux *sync.Mutex, tickerListRunning *bool) {
	currentTickers, err := api.ListTickersPerPage(page)
	if err != nil {
		log.Error("[polygon] failed to list symbols (%v)", err)
	}

	if len(currentTickers) == 0 {
		*tickerListRunning = false
		return
	}

	symbolListMux.Lock()
	for _, s := range currentTickers {
		if pattern.Match(s.Ticker) && s.Ticker != "" {
			*symbolList = append(*symbolList, s.Ticker)
		}
	}
	symbolListMux.Unlock()
}

func getBars(start time.Time, end time.Time, period time.Duration, symbol string, exchangeIDs []int, unadjusted bool, writerWP *worker.WorkerPool) {
	if len(exchangeIDs) != 0 && period != 24*time.Hour {
		log.Warn("[polygon] bar period not adjustable when exchange filtered")
		period = 24 * time.Hour
	}
	log.Info("[polygon] backfilling bars for %v", symbol)
	for end.After(start) {

		if start.Add(period).After(end) {
			period = end.Sub(start)
		}

		log.Info("[polygon] backfilling bars for %v between %s and %s", symbol, start, start.Add(period))

		if len(exchangeIDs) == 0 {
			if err := backfill.Bars(symbol, start, start.Add(period), batchSize, unadjusted, writerWP); err != nil {
				log.Warn("[polygon] failed to backfill bars for %v (%v)", symbol, err)
			}
		} else {
			if calendar.Nasdaq.IsMarketDay(start) {
				if err := backfill.BuildBarsFromTrades(symbol, start, exchangeIDs, batchSize); err != nil {
					log.Warn("[polygon] failed to backfill bars for %v @ %v (%v)", symbol, start, err)
				}
			}
		}
		start = start.Add(period)
	}
}

func getQuotes(start time.Time, end time.Time, period time.Duration, symbol string, writerWP *worker.WorkerPool) {
	log.Info("[polygon] backfilling quotes for %v", symbol)
	for end.After(start) {

		if start.Add(period).After(end) {
			period = end.Sub(start)
		}

		log.Info("[polygon] backfilling quotes for %v between %s and %s", symbol, start, start.Add(period))
		if err := backfill.Quotes(symbol, start, start.Add(period), batchSize, writerWP); err != nil {
			log.Warn("[polygon] failed to backfill quote for %v @ %v (%v)", symbol, start, err)
		}

		start = start.Add(period)
	}
}

func getTrades(start time.Time, end time.Time, period time.Duration, symbol string, writerWP *worker.WorkerPool) {
	log.Info("[polygon] backfilling trades for %v", symbol)
	for end.After(start) {

		if start.Add(period).After(end) {
			period = end.Sub(start)
		}

		log.Info("[polygon] backfilling trades for %v between %s and %s", symbol, start, start.Add(period))
		if err := backfill.Trades(symbol, start, start.Add(period), batchSize, writerWP); err != nil {
			log.Warn("[polygon] failed to backfill trades for %v @ %v (%v)", symbol, start, err)
		}

		start = start.Add(period)
	}

}

func parseAndValidateDuration(durationString string, max time.Duration, min time.Duration) (time.Duration, error) {

	duration, err := time.ParseDuration(durationString)
	if err != nil {
		return 0, err
	}
	if duration < min {
		log.Warn("duration overridden to %s because given duration (%s) subceed the minimum value)", min, duration)
		duration = min
	}
	if duration > max {
		log.Warn("duration overridden to %s because given duration (%s) exceed the maximum value)", max, duration)
		duration = max
	}
	return duration, nil
}

func unique(stringSlice []string) []string {
	var list []string
	keys := make(map[string]bool)
	for _, entry := range stringSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
