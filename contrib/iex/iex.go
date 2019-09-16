package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/contrib/iex/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

const (
	minute   = "1Min"
	daily    = "1D"
	fiveYear = "5y"
	oneDay   = "1d"
	monthly  = "1m"
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
	json.Unmarshal(data, &config)

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

	f := IEXFetcher{
		backfillM:        &sync.Map{},
		config:           config,
		queue:            make(chan []string, int(len(config.Symbols)/api.BatchSize)+1),
		lastM:            &sync.Map{},
		refreshSymbols:   len(config.Symbols) == 0,
		lastDailyRunDate: 0,
	}

	f.UpdateSymbolList()

	return &f, nil
}

func (f *IEXFetcher) UpdateSymbolList() {
	// update the symbol list if there was no static list in config
	if f.refreshSymbols {
		log.Info("refreshing symbols list from IEX")
		resp, err := api.ListSymbols()
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

	go f.workBackfill()

	// loop forever over the batches
	for batch := range f.queue {
		f.pollIntraday(batch)

		if onceDaily(&f.lastDailyRunDate, 0, 10) {
			f.UpdateSymbolList()
			f.pollDaily(batch)
		}

		<-time.After(limiter())
		f.queue <- batch
	}
}

func (f *IEXFetcher) pollIntraday(symbols []string) {
	if !f.config.Intraday {
		return
	}
	limit := 10

	resp, err := api.GetBars(symbols, oneDay, &limit, 5)
	if err != nil {
		log.Error("failed to query intraday bar batch (%v)", err)
	}

	if err = f.writeBars(resp, true, false); err != nil {
		log.Error("failed to write intraday bar batch (%v)", err)
	}
}

func (f *IEXFetcher) pollDaily(symbols []string) {
	if !f.config.Daily {
		return
	}
	limit := 1
	log.Info("running daily bars poll from IEX")
	resp, err := api.GetBars(symbols, monthly, &limit, 5)
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
			close  []float32
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

		for _, bar := range bars.Chart {
			if bar.Volume == 0 {
				continue
			}

			ts, err = bar.GetTimestamp()
			if err != nil {
				return err
			}

			if ts.IsZero() {
				continue
			}

			epoch = append(epoch, ts.Unix())
			open = append(open, bar.Open)
			high = append(high, bar.High)
			low = append(low, bar.Low)
			close = append(close, bar.Close)
			volume = append(volume, bar.Volume)
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
		cs.AddColumn("Close", close)
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

func (f *IEXFetcher) backfill(symbol, timeframe string, ts *time.Time) (err error) {
	var (
		resp     *api.GetBarsResponse
		intraday = strings.EqualFold(timeframe, minute)
	)

	if intraday {
		log.Info("Running backfill to load intraday bars")
		resp, err = api.GetBars([]string{symbol}, oneDay, nil, 5)
	} else {
		log.Info("Running backfill to load intraday bars")
		resp, err = api.GetBars([]string{symbol}, fiveYear, nil, 5)
	}

	if err != nil {
		log.Error("failed to backfill %v/%v (%v)", symbol, timeframe, err)
		return
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

	return
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
			 log.Info("backfilling [%v|%v]", symbol, timeframe)
				go func() {
					count++

					wg.Add(1)
					defer wg.Done()

					// backfill the symbol/timeframe pair in parallel
					if f.backfill(symbol, timeframe, value.(*time.Time)) == nil {
						f.backfillM.Store(key, nil)
					}
				}()
			} else {
				log.Info("skipping backfill [%v|%v]", symbol, timeframe)
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
	return time.Second / 50
}

func onceDaily(lastDailyRunDate *int, runHour int, runMinute int) bool {
	now := time.Now()

	if *lastDailyRunDate == 0 || (*lastDailyRunDate != now.Day() && runHour == now.Hour() && runMinute == now.Minute()) {
		*lastDailyRunDate = now.Day()
		return true
	} else {
		return false
	}
}

func main() {
	api.SetToken(os.Getenv("IEXTOKEN"))
	resp, err := api.GetBars([]string{"AAPL", "AMD", "X", "NVDA", "AMPY", "IBM", "GOOG"}, oneDay, nil, 5)

	if err != nil {
		panic(err)
	}

	for symbol, chart := range *resp {
		for _, bar := range chart.Chart {
			fmt.Printf("symbol: %v bar: %v\n", symbol, bar)
		}
	}

	fmt.Printf("-------------------\n\n")
	resp, err = api.GetBars([]string{"AMPY", "MSFT", "DVCR"}, oneDay, nil, 5)

	if err != nil {
		panic(err)
	}

	for symbol, chart := range *resp {
		for _, bar := range chart.Chart {
			fmt.Printf("symbol: %v bar: %v\n", symbol, bar)
		}
	}
}
