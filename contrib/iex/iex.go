package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/iex/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	minute   = "1Min"
	daily    = "1D"
	fiveYear = "5y"
	oneDay   = "1d"
	monthly  = "1m"
	retryNum = 5
)

type IEXFetcher struct {
	config           FetcherConfig
	backfillM        *sync.Map
	queue            chan []string
	lastM            *sync.Map
	refreshSymbols   bool
	lastDailyRunDate int
}

type FetcherConfig struct {
	// determines whether or not daily (1D) bars are queried
	Daily bool
	// determines whether or not intraday (1Min) bars are queried
	Intraday bool
	// list of symbols to poll - queries all if empty
	Symbols []string
	// API Token
	Token string
	// True for sandbox
	Sandbox bool
}

func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	_ = json.Unmarshal(data, &config)

	if config.Token == "" {
		return nil, fmt.Errorf("IEXCloud Token is not set")
	}

	api.SetToken(config.Token)
	api.SetSandbox(config.Sandbox)

	if config.Sandbox {
		log.Info("starting for IEX sandbox")
	} else {
		log.Info("starting for IEX production")
	}

	return &IEXFetcher{
		backfillM:        &sync.Map{},
		config:           config,
		queue:            make(chan []string, len(config.Symbols)/api.BatchSize+1),
		lastM:            &sync.Map{},
		refreshSymbols:   len(config.Symbols) == 0,
		lastDailyRunDate: 0,
	}, nil
}

func (f *IEXFetcher) UpdateSymbolList(ctx context.Context) {
	// update the symbol list if there was no static list in config
	if f.refreshSymbols {
		log.Info("refreshing symbols list from IEX")
		resp, err := api.ListSymbols(ctx)
		if err != nil {
			return
		}

		f.config.Symbols = make([]string, len(*resp))
		log.Info("Loaded list of %d symbols from IEX", len(f.config.Symbols))
		for i, s := range *resp {
			if s.IsEnabled {
				f.config.Symbols[i] = s.Symbol
			}
		}
	}
}

func (f *IEXFetcher) Run() {
	ctx := context.Background()
	// batchify the symbols & queue the batches
	f.UpdateSymbolList(ctx)
	f.queue = make(chan []string, len(f.config.Symbols)/api.BatchSize+1)

	log.Info("Launching backfill")
	go f.workBackfill(ctx)

	go func() {
		for { // loop forever adding batches of symbols to fetch
			symbols := f.config.Symbols
			for i := 0; i < len(symbols); i += api.BatchSize {
				end := i + api.BatchSize
				if end > len(symbols) {
					end = len(symbols)
				}
				f.queue <- symbols[i:end]
			}

			// Put a marker in the queue so the loop can pause til the next minute
			f.queue <- []string{"__EOL__"}
		}
	}()

	const (
		runHour   = 5
		runMinute = 10
	)
	runDaily := onceDaily(&f.lastDailyRunDate, runHour, runMinute)
	start := time.Now()
	iWorkers := make(chan bool, runtime.NumCPU())
	var iWg sync.WaitGroup
	for batch := range f.queue {
		if batch[0] == "__EOL__" {
			log.Debug("End of Symbol list.. waiting for workers")
			iWg.Wait()
			end := time.Now()
			log.Info("Minute bar fetch for %d symbols completed (elapsed %s)", len(f.config.Symbols), end.Sub(start).String())

			runDaily = onceDaily(&f.lastDailyRunDate, runHour, runMinute)
			if runDaily {
				log.Info("time for daily task(s)")
				go f.UpdateSymbolList(ctx)
			}

			delay := time.Minute - end.Sub(start)
			log.Debug("Sleep for %s", delay.String())
			<-time.After(delay)
			start = time.Now()
		} else {
			iWorkers <- true
			iWg.Add(1)
			go func() {
				defer iWg.Done()
				defer func() { <-iWorkers }()

				f.pollIntraday(ctx, batch)

				if runDaily {
					f.pollDaily(ctx, batch)
				}
			}()

			const limiter = time.Second / 50
			<-time.After(limiter)
		}
	}
}

func (f *IEXFetcher) pollIntraday(ctx context.Context, symbols []string) {
	if !f.config.Intraday {
		return
	}
	limit := 10

	start := time.Now()
	resp, err := api.GetBars(ctx, symbols, oneDay, &limit, retryNum)
	if err != nil {
		log.Error("failed to query intraday bar batch (%v)", err)
		return
	}
	fetched := time.Now()

	if err = f.writeBars(resp, true, false); err != nil {
		log.Error("failed to write intraday bar batch (%v)", err)
		return
	}
	done := time.Now()
	log.Debug("Done Batch (fetched: %s, wrote: %s)", done.Sub(fetched).String(), fetched.Sub(start).String())
}

func (f *IEXFetcher) pollDaily(ctx context.Context, symbols []string) {
	if !f.config.Daily {
		return
	}
	limit := 1
	log.Info("running daily bars poll from IEX")
	resp, err := api.GetBars(ctx, symbols, monthly, &limit, retryNum)
	if err != nil {
		log.Error("failed to query daily bar batch (%v)", err)
	}

	if err = f.writeBars(resp, false, false); err != nil {
		log.Error("failed to write daily bar batch (%v)", err)
	}
}

func (f *IEXFetcher) writeBars(resp *api.GetBarsResponse, intraday, backfill bool) error {
	if resp == nil {
		return nil
	}

	csm := io.NewColumnSeriesMap()

	for symbol, bars := range *resp {
		if len(bars.Chart) == 0 {
			continue
		}

		if backfill {
			log.Info("backfill: Writing %d bars for %s", len(bars.Chart), symbol)
		}

		var (
			tbk    *io.TimeBucketKey
			epoch  []int64
			open   []float32
			high   []float32
			low    []float32
			clos   []float32
			volume []int32
		)

		if intraday {
			tbk = io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/%s/OHLCV", symbol, minute))
		} else {
			tbk = io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/%s/OHLCV", symbol, daily))
		}

		var (
			ts  time.Time
			err error
		)

		for i := range bars.Chart {
			if bars.Chart[i].Volume == 0 {
				continue
			}

			ts, err = bars.Chart[i].GetTimestamp()
			if err != nil {
				return err
			}

			if ts.IsZero() {
				continue
			}

			epoch = append(epoch, ts.Unix())
			open = append(open, bars.Chart[i].Open)
			high = append(high, bars.Chart[i].High)
			low = append(low, bars.Chart[i].Low)
			clos = append(clos, bars.Chart[i].Close)
			volume = append(volume, bars.Chart[i].Volume)
		}

		if len(epoch) == 0 {
			continue
		}

		// determine whether we skip the bar so we don't
		// re-stream bars that have already been written
		if !backfill {
			v, ok := f.lastM.Load(*tbk)
			if ok && v.(int64) >= epoch[len(epoch)-1] {
				continue
			}
		}

		f.backfillM.LoadOrStore(strings.Replace(tbk.GetItemKey(), "/OHLCV", "", 1), &ts)

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epoch)
		cs.AddColumn("Open", open)
		cs.AddColumn("High", high)
		cs.AddColumn("Low", low)
		cs.AddColumn("Close", clos)
		cs.AddColumn("Volume", volume)
		csm.AddColumnSeries(*tbk, cs)
	}

	if err := executor.WriteCSM(csm, false); err != nil {
		return err
	}

	f.updateLastWritten(&csm)

	return nil
}

func (f *IEXFetcher) updateLastWritten(csm *io.ColumnSeriesMap) {
	if csm == nil {
		return
	}

	for tbk, cs := range *csm {
		epoch := cs.GetEpoch()
		if len(epoch) == 0 {
			continue
		}

		f.lastM.Store(tbk, epoch[len(epoch)-1])
	}
}

func (f *IEXFetcher) backfill(ctx context.Context, symbol, timeframe string) (err error) {
	var (
		resp     *api.GetBarsResponse
		intraday = strings.EqualFold(timeframe, minute)
	)

	if intraday {
		resp, err = api.GetBars(ctx, []string{symbol}, oneDay, nil, retryNum)
	} else {
		resp, err = api.GetBars(ctx, []string{symbol}, fiveYear, nil, retryNum)
	}

	if err != nil {
		log.Error("failed to backfill %v/%v (%v)", symbol, timeframe, err)
		return err
	}

	// c := (*resp)[symbol].Chart

	// if len(c) > 0 {
	// 	log.Info(
	// 		"backfilling %v/%v (%v bars | start: %v-%v | end: %v-%v)",
	// 		symbol, timeframe,
	// 		len(c), c[0].Date,
	// 		c[0].Minute, c[len(c)-1].Date,
	// 		c[len(c)-1].Minute)
	// }

	if err = f.writeBars(resp, intraday, true); err != nil {
		log.Error("failed to write bars from backfill for %v/%v (%v)", symbol, timeframe, err)
	}

	return err
}

func (f *IEXFetcher) workBackfill(ctx context.Context) {
	const tickInterval = 30 * time.Second
	ticker := time.NewTicker(tickInterval)

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
				log.Info("backfilling [%v|%v]", symbol, timeframe)
				go func() {
					count++

					wg.Add(1)
					defer wg.Done()

					// backfill the symbol/timeframe pair in parallel
					if f.backfill(ctx, symbol, timeframe) == nil {
						f.backfillM.Store(key, nil)
					}
				}()
			} else {
				log.Debug("skipping backfill [%v|%v]", symbol, timeframe)
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

func onceDaily(lastDailyRunDate *int, runHour, runMinute int) bool {
	now := time.Now()

	if *lastDailyRunDate == 0 || (*lastDailyRunDate != now.Day() && runHour == now.Hour() && runMinute <= now.Minute()) {
		*lastDailyRunDate = now.Day()
		return true
	}
	return false
}

func main() {
	ctx := context.Background()
	api.SetToken(os.Getenv("IEXTOKEN"))
	resp, err := api.GetBars(ctx, []string{"AAPL", "AMD", "X", "NVDA", "AMPY", "IBM", "GOOG"}, oneDay, nil, retryNum)
	if err != nil {
		panic(err)
	}

	for symbol, chart := range *resp {
		for i := range chart.Chart {
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf("symbol: %v bar: %v\n", symbol, chart.Chart[i])
		}
	}

	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("-------------------\n\n")
	resp, err = api.GetBars(ctx, []string{"AMPY", "MSFT", "DVCR"}, oneDay, nil, retryNum)

	if err != nil {
		panic(err)
	}

	for symbol, chart := range *resp {
		for i := range chart.Chart {
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf("symbol: %v bar: %v\n", symbol, chart.Chart[i])
		}
	}
}
