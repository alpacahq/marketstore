package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/contrib/calendar"
	"github.com/alpacahq/marketstore/contrib/iex/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

type IEXFetcher struct {
	config    FetcherConfig
	backfillM *sync.Map
	queue     chan []string
}

type FetcherConfig struct {
	// determines whether or not daily (1D) bars are queried
	Daily bool
	// determines whether or not intraday (1Min) bars are queried
	Intraday bool

	Symbols []string
}

func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	json.Unmarshal(data, &config)

	// grab the symbol list if none are specified
	if len(config.Symbols) > 0 {
		resp, err := api.ListSymbols()
		if err != nil {
			return nil, err
		}

		config.Symbols = make([]string, len(*resp))

		for i, s := range *resp {
			config.Symbols[i] = s.Symbol
		}
	}

	return &IEXFetcher{
		backfillM: &sync.Map{},
		config:    config,
		queue:     make(chan []string, len(config.Symbols)%api.BatchSize+1),
	}, nil
}

func (f *IEXFetcher) Run() {
	// batchify the symbols & queue the batches
	{
		symbols := f.config.Symbols

		for i := 0; i < len(symbols); i += api.BatchSize {
			end := i + api.BatchSize
			if end > len(symbols) {
				end = len(symbols)
			}

			f.queue <- symbols[i:end]
		}
	}

	// loop forever over the batches
	for batch := range f.queue {
		f.pollIntraday(batch)

		if !calendar.Nasdaq.IsMarketOpen(time.Now()) {
			f.pollDaily(batch)
		}

		<-time.After(limiter())
		f.queue <- batch
	}
}

func (f *IEXFetcher) pollIntraday(symbols []string) {
	limit := 1

	resp, err := api.GetBars(symbols, "1d", &limit, 5)
	if err != nil {
		log.Error("failed to query intraday bar batch (%v)", err)
	}

	if err = f.writeBars(resp, true); err != nil {
		log.Error("failed to write intraday bar batch (%v)", err)
	}
}

func (f *IEXFetcher) pollDaily(symbols []string) {
	limit := 1

	resp, err := api.GetBars(symbols, "1m", &limit, 5)
	if err != nil {
		log.Error("failed to query intraday bar batch (%v)", err)
	}

	if err = f.writeBars(resp, true); err != nil {
		log.Error("failed to write intraday bar batch (%v)", err)
	}
}

func (f *IEXFetcher) writeBars(resp *api.GetBarsResponse, intraday bool) error {
	if resp == nil {
		return nil
	}

	csm := io.NewColumnSeriesMap()

	for symbol, bars := range *resp {
		if len(bars.Chart) == 0 {
			continue
		}

		var tbk *io.TimeBucketKey

		if intraday {
			tbk = io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
		} else {
			tbk = io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1D/OHLCV", symbol))
		}

		epoch := make([]int64, len(bars.Chart))
		open := make([]float32, len(bars.Chart))
		high := make([]float32, len(bars.Chart))
		low := make([]float32, len(bars.Chart))
		close := make([]float32, len(bars.Chart))
		volume := make([]int32, len(bars.Chart))

		var (
			ts  time.Time
			err error
		)

		for i, bar := range bars.Chart {
			ts, err = bar.GetTimestamp()
			if err != nil {
				return err
			}

			epoch[i] = ts.Unix()
			open[i] = bar.Open
			high[i] = bar.High
			low[i] = bar.Low
			close[i] = bar.Close
			volume[i] = bar.Volume
		}

		f.backfillM.LoadOrStore(strings.Replace(tbk.String(), "/OHLCV", "", 1), &ts)

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epoch)
		cs.AddColumn("Open", open)
		cs.AddColumn("High", high)
		cs.AddColumn("Low", low)
		cs.AddColumn("Close", close)
		cs.AddColumn("Volume", volume)
		csm.AddColumnSeries(*tbk, cs)
	}

	return executor.WriteCSM(csm, false)
}

func (f *IEXFetcher) backfill(symbol, timeframe string, ts *time.Time) {
	var (
		err      error
		resp     *api.GetBarsResponse
		intraday = timeframe == "1D"
	)

	if intraday {
		resp, err = api.GetBars([]string{symbol}, "1d", nil, 5)
	} else {
		resp, err = api.GetBars([]string{symbol}, "5y", nil, 5)
	}

	if err != nil {
		log.Error("failed to backfill %v/%v (%v)", symbol, timeframe, err)
		return
	}

	if err = f.writeBars(resp, intraday); err != nil {
		log.Error("failed to write bars from backfill for %v/%v (%v)", symbol, timeframe, err)
	}
}

func (f *IEXFetcher) workBackfill() {
	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {
		wg := sync.WaitGroup{}
		count := 0

		// range over symbols that need backfilling, and
		// backfill them from the last written record
		f.backfillM.Range(func(key, value interface{}) bool {
			parts := strings.Split(key.(string), "/")
			symbol := parts[0]
			timeframe := parts[1]

			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				go func() {
					wg.Add(1)
					defer wg.Done()

					// backfill the symbol/timeframe pair in parallel
					f.backfill(symbol, timeframe, value.(*time.Time))
					f.backfillM.Store(key, nil)
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

func limiter() time.Duration {
	if calendar.Nasdaq.IsMarketOpen(time.Now()) {
		return time.Second / 100
	}

	return time.Second / 50
}

func main() {}
