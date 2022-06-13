package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/gobwas/glob"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
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
	// NY, _          = time.LoadLocation("America/New_York").
	configFilePath string

	format = "2006-01-02"
)

const (
	defaultBatchSize           = 50000
	defaultMaxConnsPerHost     = 100
	defaultMaxIdleConnsPerHost = 100
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory to backfill to. If empty, the dir is taken from mkts.yml")
	flag.StringVar(&from, "from", time.Now().Add(-365*24*time.Hour).Format(format),
		"backfill from date (YYYY-MM-DD) [included]",
	)
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
	flag.IntVar(&batchSize, "batchSize", defaultBatchSize, "batch/pagination size for downloading trades, quotes, & bars")
	flag.StringVar(&apiKey, "apiKey", "", "polygon API key")
	flag.StringVar(&cacheDir, "cache-dir", "", "directory to dump polygon's json replies")
	flag.BoolVar(&readFromCache, "read-from-cache", false, "read cached results if available")
	flag.BoolVar(&noIngest, "no-ingest", false, "do not ingest downloaded data, just store it in cache")
	flag.BoolVar(&unadjusted, "unadjusted", false, "request unadjusted price data")
	flag.StringVar(&configFilePath, "config", "/etc/mkts.yml", "path to the mkts.yml config file")

	flag.Parse()
}

// nolint:funlen,gocognit,gocyclo // TODO: refactor the main func
func main() {
	const allPerm = 0o777
	const oneDay = 24 * time.Hour
	rootDir, triggers, walRotateInterval := initConfig()
	instanceMeta, walWG, err := initWriter(rootDir, triggers, walRotateInterval)
	if err != nil {
		log.Error("failed to set up new instance config. err=" + err.Error())
		os.Exit(1)
	}

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
		log.Error("[polygon] api key is required")
		os.Exit(1)
	}

	if noIngest && cacheDir == "" {
		log.Error("[polygon] no-ingest should only be specified when cache-dir is set")
		os.Exit(1)
	}
	backfill.NoIngest = noIngest

	api.SetAPIKey(apiKey)

	start, err := time.Parse(format, from)
	if err != nil {
		log.Error("[polygon] failed to parse from timestamp (%v)", err)
		os.Exit(1)
	}

	end, err := time.Parse(format, to)
	if err != nil {
		log.Error("[polygon] failed to parse to timestamp (%v)", err)
		os.Exit(1)
	}

	tradePeriodDuration, err := parseAndValidateDuration(tradePeriod, 60*oneDay, oneDay)
	if err != nil {
		log.Error("[polygon] failed to parse trade-period duration (%v)", err)
		os.Exit(1)
	}

	quotePeriodDuration, err := parseAndValidateDuration(quotePeriod, 60*oneDay, oneDay)
	if err != nil {
		log.Error("[polygon] failed to parse trade-period duration (%v)", err)
		os.Exit(1)
	}

	barPeriodDuration, err := parseAndValidateDuration(barPeriod, 60*oneDay, oneDay)
	if err != nil {
		log.Error("[polygon] failed to parse trade-period duration (%v)", err)
		os.Exit(1)
	}

	if cacheDir != "" {
		err = os.MkdirAll(cacheDir, allPerm)
		if err != nil {
			log.Error("[polygon] cannot create json dump directory (%v)", err)
			os.Exit(1)
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

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
		},
		Timeout: 10 * time.Second,
	}

	for page := 0; tickerListRunning; page++ {
		currentPage := page

		tickerListWP.Do(func() {
			getTicker(client, currentPage, pattern, &symbolList, symbolListMux, &tickerListRunning)
		})
	}

	tickerListWP.CloseAndWait()
	symbolList = unique(symbolList)
	sort.Strings(symbolList)
	if len(symbolList) == 0 {
		log.Error("no symbol selected")
		os.Exit(1)
	}
	log.Info("[polygon] selected %v symbols", len(symbolList))

	var exchangeIDs []int
	if exchanges != "*" {
		for _, exchangeIDStr := range strings.Split(exchanges, ",") {
			exchangeIDInt, err := strconv.Atoi(exchangeIDStr)
			if err != nil {
				log.Error("Invalid exchange ID: %v", exchangeIDStr)
				os.Exit(1)
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
				getBars(client, start, end, barPeriodDuration, currentSymbol, exchangeIDs, unadjusted, writerWP)
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
				getQuotes(client, start, end, quotePeriodDuration, currentSymbol, writerWP)
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
				getTrades(client, start, end, tradePeriodDuration, currentSymbol, writerWP)
			})
		}

		log.Info("[polygon] wait for api workers")
		apiCallerWP.CloseAndWait()
		log.Info("[polygon] wait for writer workers")
		writerWP.CloseAndWait()
	}

	log.Info("[polygon] wait for shutdown")
	instanceMeta.WALFile.TriggerShutdown()
	walWG.Wait()
	instanceMeta.WALFile.FinishAndWait()

	log.Info("[polygon] api call time %s", backfill.APICallTime)
	log.Info("[polygon] wait time %s", backfill.WaitTime)
	log.Info("[polygon] write time %s", backfill.WriteTime)
	log.Info("[polygon] backfilling complete %s", time.Since(startTime))
}

func initConfig() (rootDir string, triggers []*utils.TriggerSetting, walRotateInterval int) {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Error("failed to read configuration file error: %s", err.Error())
		os.Exit(1)
	}

	config, err := utils.ParseConfig(data)
	if err != nil {
		log.Error("failed to parse configuration file error: %v", err.Error())
		os.Exit(1)
	}
	utils.InstanceConfig = *config // TODO: remove the singleton instance

	return config.RootDirectory, config.Triggers, config.WALRotateInterval
}

func initWriter(rootDir string, triggers []*utils.TriggerSetting, walRotateInterval int,
) (instanceConfig *executor.InstanceMetadata, walWG *sync.WaitGroup, err error) {
	// if configured, also load the ondiskagg triggers
	var tm []*trigger.Matcher
	for _, triggerSetting := range triggers {
		if triggerSetting.Module == "ondiskagg.so" {
			tmatcher := trigger.NewTriggerMatcher(triggerSetting)
			tm = append(tm, tmatcher)
			break
		}
	}

	instanceConfig, walWG, err = executor.NewInstanceSetup(rootDir, nil, tm, walRotateInterval,
		executor.WALBypass(true))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create instance setup for polygon/backfill: %w", err)
	}

	return instanceConfig, walWG, nil
}

func getTicker(client *http.Client, page int, pattern glob.Glob, symbolList *[]string, symbolListMux *sync.Mutex,
	tickerListRunning *bool,
) {
	currentTickers, err := api.ListTickersPerPage(client, page)
	if err != nil {
		log.Error("[polygon] failed to list symbols (%v)", err)
	}

	if len(currentTickers) == 0 {
		*tickerListRunning = false
		return
	}

	symbolListMux.Lock()
	for i := range currentTickers {
		if pattern.Match(currentTickers[i].Ticker) && currentTickers[i].Ticker != "" {
			*symbolList = append(*symbolList, currentTickers[i].Ticker)
		}
	}
	symbolListMux.Unlock()
}

func getBars(client *http.Client, start, end time.Time, period time.Duration, symbol string, exchangeIDs []int,
	unadjusted bool, writerWP *worker.Pool,
) {
	const oneDay = 24 * time.Hour
	if len(exchangeIDs) != 0 && period != oneDay {
		log.Warn("[polygon] bar period not adjustable when exchange filtered")
		period = oneDay
	}
	log.Info("[polygon] backfilling bars for %v", symbol)
	for end.After(start) {
		if start.Add(period).After(end) {
			period = end.Sub(start)
		}

		log.Info("[polygon] backfilling bars for %v between %s and %s", symbol, start, start.Add(period))

		if len(exchangeIDs) == 0 {
			err := backfill.Bars(client, symbol, start, start.Add(period), batchSize, unadjusted, writerWP)
			if err != nil {
				log.Warn("[polygon] failed to backfill bars for %v (%v)", symbol, err)
			}
		} else if calendar.Nasdaq.IsMarketDay(start) {
			if err := backfill.BuildBarsFromTrades(client, symbol, start, exchangeIDs, batchSize); err != nil {
				log.Warn("[polygon] failed to backfill bars for %v @ %v (%v)", symbol, start, err)
			}
		}
		start = start.Add(period)
	}
}

// resourceName = {"quotes", "trades"}, and it's just for logging.
func getQuotesOrTrades(resourceName string, client *http.Client, start, end time.Time, period time.Duration,
	symbol string, writerWP *worker.Pool,
) {
	log.Info(fmt.Sprintf("[polygon] backfilling %s for %v", resourceName, symbol))
	for end.After(start) {
		if start.Add(period).After(end) {
			period = end.Sub(start)
		}

		log.Info(fmt.Sprintf("[polygon] backfilling %s for %v between %s and %s",
			resourceName, symbol, start, start.Add(period)),
		)
		if err := backfill.Quotes(client, symbol, start, start.Add(period), batchSize, writerWP); err != nil {
			log.Warn(fmt.Sprintf("[polygon] failed to backfill %s for %v @ %v (%v)",
				resourceName, symbol, start, err),
			)
		}

		start = start.Add(period)
	}
}

func getQuotes(client *http.Client, start, end time.Time, period time.Duration, symbol string, writerWP *worker.Pool) {
	getQuotesOrTrades("quotes", client, start, end, period, symbol, writerWP)
}

func getTrades(client *http.Client, start, end time.Time, period time.Duration, symbol string, writerWP *worker.Pool) {
	getQuotesOrTrades("trades", client, start, end, period, symbols, writerWP)
}

func parseAndValidateDuration(durationString string, max, min time.Duration) (time.Duration, error) {
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
