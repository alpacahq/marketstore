package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
	gdax "github.com/preichenberger/go-gdax"
)

type ByTime []gdax.HistoricRate

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Time.Before(a[j].Time) }

// FetchConfig is the configuration for GdaxFetcher you can define in
// marketstore's config file through bgworker extension.
type FetcherConfig struct {
	// list of currency symbols, defults to ["BTC", "ETH", "LTC", "BCH"]
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from an hour ago by default
	QueryStart string `json:"query_start"`
	// such as 5Min, 1D.  defaults to 1Min
	BaseTimeframe string `json:"base_timeframe"`
}

// GdaxFetcher is the main worker instance.  It implements bgworker.Run().
type GdaxFetcher struct {
	config        map[string]interface{}
	symbols       []string
	queryStart    time.Time
	baseTimeframe *utils.Timeframe
}

func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

// NewBgWorker returns the new instance of GdaxFetcher.  See FetcherConfig
// for the details of available configurations.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	symbols := []string{"ETH", "LTC", "BCH"}

	config := recast(conf)
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
	var queryStart time.Time
	if config.QueryStart != "" {
		trials := []string{
			"2006-01-02 03:04:05",
			"2006-01-02T03:04:05",
			"2006-01-02 03:04",
			"2006-01-02T03:04",
			"2006-01-02",
		}
		for _, layout := range trials {
			qs, err := time.Parse(layout, config.QueryStart)
			if err == nil {
				queryStart = qs.In(utils.InstanceConfig.Timezone)
				break
			}
		}
	}
	timeframeStr := "1Min"
	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}
	return &GdaxFetcher{
		config:        conf,
		symbols:       symbols,
		queryStart:    queryStart,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

func findLastTimestamp(symbol string, tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)
	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start.Unix(), end.Unix())
	query.SetRowLimit(io.LAST, 1)
	parsed, err := query.Parse()
	if err != nil {
		return time.Time{}
	}
	reader, err := executor.NewReader(parsed)
	csm, _, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts := cs.GetTime()
	return ts[0]
}

// Run() runs forever to get public historical rate for each configured symbol,
// and writes in marketstore data format.  In case any error including rate limit
// is returned from GDAX, it waits for a minute.
func (gd *GdaxFetcher) Run() {
	symbols := gd.symbols
	client := gdax.NewClient("", "", "")
	timeStart := time.Time{}
	for _, symbol := range symbols {
		tbk := io.NewTimeBucketKey("gdax_btc_" + symbol + "/" + gd.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(symbol, tbk)
		glog.Infof("lastTimestamp for %s = %v", symbol, lastTimestamp)
		if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
			timeStart = lastTimestamp
		}
	}
	if timeStart.IsZero() {
		if !gd.queryStart.IsZero() {
			timeStart = gd.queryStart
		} else {
			timeStart = time.Now().UTC().Add(-time.Hour)
		}
	}
	for {
		timeEnd := timeStart.Add(gd.baseTimeframe.Duration * 300)
		lastTime := timeStart
		for _, symbol := range symbols {
			params := gdax.GetHistoricRatesParams{
				Start:       timeStart,
				End:         timeEnd,
				Granularity: int(gd.baseTimeframe.Duration.Seconds()),
			}
			glog.Infof("Requesting %s %v - %v", symbol, timeStart, timeEnd)
			rates, err := client.GetHistoricRates(symbol+"-BTC", params)
			if err != nil {
				glog.Errorf("Response error: %v", err)
				// including rate limit case
				time.Sleep(time.Minute)
				continue
			}
			if len(rates) == 0 {
				glog.Info("len(rates) == 0")
				continue
			}
			epoch := make([]int64, 0)
			open := make([]float64, 0)
			high := make([]float64, 0)
			low := make([]float64, 0)
			close := make([]float64, 0)
			volume := make([]float64, 0)
			sort.Sort(ByTime(rates))
			for _, rate := range rates {
				if rate.Time.After(lastTime) {
					lastTime = rate.Time
				}
				epoch = append(epoch, rate.Time.Unix())
				open = append(open, float64(rate.Open))
				high = append(high, float64(rate.High))
				low = append(low, float64(rate.Low))
				close = append(close, float64(rate.Close))
				volume = append(volume, rate.Volume)
			}
			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			glog.Infof("%s: %d rates between %v - %v", symbol, len(rates),
				rates[0].Time, rates[(len(rates))-1].Time)
			csm := io.NewColumnSeriesMap()
			// creslin change from symbol to exchange_symbol_quote
			tbk := io.NewTimeBucketKey("gdax_btc_" + symbol + "/" + gd.baseTimeframe.String + "/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, false)
		}
		// next fetch start point
		timeStart = lastTime.Add(gd.baseTimeframe.Duration)
		// for the next bar to complete, add it once more
		nextExpected := timeStart.Add(gd.baseTimeframe.Duration)
		now := time.Now()
		toSleep := nextExpected.Sub(now)
		glog.Infof("next expected(%v) - now(%v) = %v", nextExpected, now, toSleep)
		if toSleep > 0 {
			glog.Infof("Sleep for %v", toSleep)
			time.Sleep(toSleep)
		} else if time.Now().Sub(lastTime) < time.Hour {
			// let's not go too fast if the catch up is less than an hour
			time.Sleep(time.Second * 2)
		}
	}
}

func main() {

	client := gdax.NewClient("", "", "")
	params := gdax.GetHistoricRatesParams{
		Start:       time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2017, 12, 1, 1, 0, 0, 0, time.UTC),
		Granularity: 60,
	}
	res, err := client.GetHistoricRates("LTC-BTC", params)
	fmt.Println(res, err)
}

