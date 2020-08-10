package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/alpacahq/marketstore/v4/cmd/start"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/polygon_config"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	configFilePath       string
	config               polygon_config.FetcherConfig
	startTime, endTime   time.Time
	bars, quotes, trades bool
	parallelism          int
	batchSize            int

	format = "2006-01-02"
)

func init() {
	flag.StringVar(&configFilePath, "polygon_config", "./mkts.yml", "path to the mkts.yml polygon_config file")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(), "parallelism (default NumCPU)")
	flag.IntVar(&batchSize, "batchSize", 50000, "batch/pagination size for downloading trades & quotes")

	flag.Parse()
}

func main() {
	initConfig()
	initWriter()

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

	var symbolList []string
	if len(config.Symbols) > 0 {
		symbolList = config.Symbols
	} else {
		log.Info("[polygon] fetching available symbols")
		resp, err := api.ListTickers()
		if err != nil {
			log.Fatal("[polygon] failed to list symbols (%v)", err)
		}

		symbolList := make([]string, 1)
		for _, s := range resp.Tickers {
			symbolList = append(symbolList, s.Ticker)
		}
	}
	log.Info("[polygon] selected %v symbols", len(symbolList))

	if len(config.DataTypes) == 0 {
		bars = true
	} else {
		for _, dt := range config.DataTypes {
			switch dt {
			case "bars":
				bars = true
			case "quotes":
				quotes = true
			case "trades":
				trades = true
			}
		}
	}

	sem := make(chan struct{}, parallelism)

	if bars {
		log.Info("[polygon] backfilling bars from %v to %v", startTime.Format(format), endTime.Format(format))

		for _, sym := range symbolList {
			s := startTime
			e := endTime

			log.Info("[polygon] backfilling bars for %v", sym)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(s) {
					log.Info("[polygon] backfilling bars for %v on %v", sym, s.Format(format))

					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err := backfill.Bars(sym, t, t.Add(24*time.Hour)); err != nil {
							log.Warn("[polygon] failed to backfill bars for %v on %v (%v)", sym, t.Format(format), err)
						}
					}(s)
				}
				s = s.Add(24 * time.Hour)
			}
		}
	}

	if quotes {
		log.Info("[polygon] backfilling quotes from %v to %v", startTime.Format(format), endTime.Format(format))

		for _, sym := range symbolList {
			s := startTime
			e := endTime

			log.Info("[polygon] backfilling quotes for %v", sym)

			for e.After(s) {
				if calendar.Nasdaq.IsMarketDay(s) {
					log.Info("[polygon] backfilling quotes for %v on %v", sym, s.Format(format))

					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err := backfill.Quotes(sym, t, t.Add(24*time.Hour), batchSize); err != nil {
							log.Warn("[polygon] failed to backfill quotes for %v on %v (%v)", sym, t.Format(format), err)
						}
					}(s)
				}
				s = s.Add(24 * time.Hour)
			}
		}
	}

	if trades {
		log.Info("[polygon] backfilling trades from %v to %v", startTime.Format(format), endTime.Format(format))

		for _, sym := range symbolList {
			s := startTime
			e := endTime

			log.Info("[polygon] backfilling trades for %v", sym)

			for e.After(s) {
				log.Info("Checking %v", s)
				if calendar.Nasdaq.IsMarketDay(s) {
					log.Info("[polygon] backfilling trades for %v on %v", sym, s.Format(format))

					sem <- struct{}{}
					go func(t time.Time) {
						defer func() { <-sem }()

						if err := backfill.Trades(sym, t, batchSize); err != nil {
							log.Warn("[polygon] failed to backfill trades for %v on %v (%v)", sym, t.Format(format), err)
						}
					}(e)
				}
				s = s.Add(24 * time.Hour)
			}
		}
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	log.Info("[polygon] backfilling complete")

	if len(executor.ThisInstance.TriggerMatchers) > 0 {
		log.Info("[polygon] waiting for 10 more seconds for ondiskagg triggers to complete")
		time.Sleep(10 * time.Second)
	}
}

func initConfig() {
	// Attempt to read mkts.yml config file
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatal("failed to read configuration file error: %s", err.Error())
		os.Exit(1)
	}

	// Attempt to set configuration
	err = utils.InstanceConfig.Parse(data)
	if err != nil {
		log.Fatal("failed to parse configuration file error: %v", err.Error())
		os.Exit(1)
	}

	// Attempt to set the polygon plugin config settings
	foundPolygonConfig := false
	for _, bgConfig := range utils.InstanceConfig.BgWorkers {
		if bgConfig.Name == "Polygon" {
			data, _ := json.Marshal(bgConfig.Config)
			if err = json.Unmarshal(data, &config); err != nil {
				log.Fatal("failed to parse configuration file error: %v", err.Error())
				os.Exit(1)
			}
			foundPolygonConfig = true
			break
		}
	}
	if !foundPolygonConfig {
		log.Fatal("polygon background worker is not configured in %s", configFilePath)
		os.Exit(1)
	}

	if config.APIKey == "" {
		log.Fatal("[polygon] api key is required")
		os.Exit(1)
	}
	api.SetAPIKey(config.APIKey)

	startTime, err = time.Parse(format, config.QueryStart)
	if err != nil {
		log.Fatal("[polygon] failed to parse from timestamp (%v)", err)
		os.Exit(1)
	}

	t := time.Now()
	endTime = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Round(time.Minute)
}

func initWriter() {
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true, true)

	// if configured, also load the 1Min ondiskagg trigger
	for _, triggerSetting := range utils.InstanceConfig.Triggers {
		if triggerSetting.Module == "ondiskagg.so" && triggerSetting.On == "*/1Min/OHLCV" {
			tmatcher := start.NewTriggerMatcher(triggerSetting)
			executor.ThisInstance.TriggerMatchers = append(executor.ThisInstance.TriggerMatchers, tmatcher)
			break
		}
	}
}
