package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
	gdax "github.com/preichenberger/go-gdax"
)

type ByTime []gdax.HistoricRate

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Time.Before(a[j].Time) }

type GdaxFetcher struct {
	config map[string]interface{}
}

func NewBgWorker(config map[string]interface{}) (bgworker.BgWorker, error) {
	return &GdaxFetcher{}, nil
}

func (gd *GdaxFetcher) Run() {
	symbols := []string{
		"BTC", "ETH", "LTC", "BCH",
	}
	client := gdax.NewClient("", "", "")
	timeStart := time.Now().UTC().Add(-time.Hour)
	for {
		timeEnd := timeStart.Add(time.Hour)
		lastTime := timeStart
		for _, symbol := range symbols {
			params := gdax.GetHistoricRatesParams{
				Start:       timeStart,
				End:         timeEnd,
				Granularity: 60,
			}
			glog.Infof("Requesting %s %v - %v", symbol, timeStart, timeEnd)
			rates, err := client.GetHistoricRates(symbol+"-USD", params)
			if err != nil {
				glog.Errorf("response error: %v", err)
				// including rate limit case
				time.Sleep(time.Minute)
				continue
			}
			epoch := make([]int64, 0)
			open := make([]float32, 0)
			high := make([]float32, 0)
			low := make([]float32, 0)
			close := make([]float32, 0)
			volume := make([]float64, 0)
			sort.Sort(ByTime(rates))
			glog.Infof("%s: rates[0] = %v, rates[-1] = %v", symbol, rates[0].Time, rates[(len(rates))-1].Time)
			for _, rate := range rates {
				if rate.Time.After(lastTime) {
					lastTime = rate.Time
				}
				epoch = append(epoch, rate.Time.Unix())
				open = append(open, float32(rate.Open))
				high = append(high, float32(rate.High))
				low = append(low, float32(rate.Low))
				close = append(close, float32(rate.Close))
				volume = append(volume, rate.Volume)
			}
			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			csm := io.NewColumnSeriesMap()
			tbk := io.NewTimeBucketKey(symbol + "/1Min/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, false)
		}
		timeStart = lastTime
		// minute bar start + 1 minute (to the next) + 1 minute (for the last to complete)
		nextExpected := lastTime.Add(2 * time.Minute)
		now := time.Now()
		toSleep := nextExpected.Sub(now)
		glog.Infof("next expected(%v) - now(%v) = %v", nextExpected, now, toSleep)
		if toSleep > 0 {
			glog.Infof("sleep for %v", toSleep)
			time.Sleep(toSleep)
		} else if time.Now().Sub(lastTime) < time.Hour {
			// let's not go too fast if the catch up is less than an hour
			time.Sleep(time.Second)
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
	res, err := client.GetHistoricRates("BTC-USD", params)
	fmt.Println(res, err)
}
