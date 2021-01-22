package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	bitmex "github.com/alpacahq/marketstore/v4/contrib/bitmexfeeder/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// FetcherConfig is the configuration for BitmexFetcher you can define in
// marketstore's config file through bgworker extension.
type FetcherConfig struct {
	// list of currency symbols, defults to all symbols available to BitMEX
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from an hour ago by default
	QueryStart string `json:"query_start"`
	// such as 5m, 1h, 1D.  defaults to 1m
	BaseTimeframe string `json:"base_timeframe"`
}

// BitmexFetcher is the main worker instance.  It implements bgworker.Run().
type BitmexFetcher struct {
	client        *bitmex.BitmexClient
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
	client := bitmex.Init()
	symbols, err := client.GetInstruments()
	if err != nil {
		return nil, err
	}

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
	} else {
		queryStart = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	timeframeStr := "1m"
	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}
	return &BitmexFetcher{
		client:        &client,
		config:        conf,
		symbols:       symbols,
		queryStart:    queryStart,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

func findLastTimestamp(tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)
	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start, end)
	query.SetRowLimit(io.LAST, 1)
	parsed, err := query.Parse()
	if err != nil {
		return time.Time{}
	}
	reader, err := executor.NewReader(parsed, utils.InstanceConfig.DisableVariableCompression)
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts, err := cs.GetTime()
	return ts[0]
}

// Run runs forever to get public historical rate for each configured symbol,
// and writes in marketstore data format.  In case any error including rate limit
// is returned from bitMEX, it waits for a minute.
func (gd *BitmexFetcher) Run() {
	symbols := gd.symbols
	timeStart := time.Time{}
	for _, symbol := range symbols {
		symbolDir := fmt.Sprintf("bitmex_%s", symbol)
		tbk := io.NewTimeBucketKey(symbolDir + "/" + gd.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(tbk)
		log.Info("lastTimestamp for %s = %v\n", symbol, lastTimestamp)
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
		lastTime := timeStart
		for _, symbol := range symbols {
			log.Info("Requesting %s %v with 500 time periods", symbol, timeStart)
			rates, err := gd.client.GetBuckets(symbol, timeStart, gd.baseTimeframe.String)
			if err != nil {
				log.Info("Response error: %v", err)
				// including rate limit case
				time.Sleep(time.Minute)
				continue
			}
			if len(rates) == 0 {
				log.Info("len(rates) == 0")
				continue
			}
			epoch := make([]int64, 0)
			open := make([]float64, 0)
			high := make([]float64, 0)
			low := make([]float64, 0)
			close := make([]float64, 0)
			volume := make([]float64, 0)
			for _, rate := range rates {
				parsedTime, err := time.Parse(time.RFC3339, rate.Timestamp)
				if err != nil {
					panic(err)
				}
				if parsedTime.After(lastTime) {
					lastTime = parsedTime
				}
				epoch = append(epoch, parsedTime.Unix())
				open = append(open, rate.Open)
				high = append(high, rate.High)
				low = append(low, rate.Low)
				close = append(close, rate.Close)
				volume = append(volume, rate.Volume)
			}
			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			log.Debug("%s: %d rates between %s - %s", symbol, len(rates),
				rates[0].Timestamp, rates[(len(rates))-1].Timestamp)
			csm := io.NewColumnSeriesMap()
			symbolDir := fmt.Sprintf("bitmex_%s", symbol)
			tbk := io.NewTimeBucketKey(symbolDir + "/" + gd.baseTimeframe.String + "/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, false)
		}
		// next fetch start point
		timeStart = lastTime.Add(gd.baseTimeframe.Duration)
		// for the next bar to complete, add it once more
		nextExpected := timeStart.Add(gd.baseTimeframe.Duration)
		now := time.Now()
		toSleep := nextExpected.Sub(now)
		log.Info("next expected(%v) - now(%v) = %v", nextExpected, now, toSleep)

		if toSleep > 0 {
			log.Info("sleep for %v", toSleep)
			time.Sleep(toSleep)
		} else if time.Now().Sub(lastTime) < time.Hour {
			// let's not go too fast if the catch up is less than an hour
			time.Sleep(time.Second)
		}
	}
}

func main() {
	client := bitmex.Init()
	start := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	res, err := client.GetBuckets("XBTUSD", start, "5m")
	fmt.Println(res, err)
}
