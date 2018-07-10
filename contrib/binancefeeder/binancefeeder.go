package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	binance "github.com/adshao/go-binance"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
)

var suffixBinanceDefs = map[string]string{
	"Min": "m",
	"H":   "h",
	"D":   "d",
	"W":   "w",
}

//For ConvertStringToFloat function and Run() function to making exiting easier
var errorsConversion []error

type FetcherConfig struct {
	Symbols       []string `json:"symbols"`
	BaseCurrency  string   `json:"base_currency"`
	QueryStart    string   `json:"query_start"`
	QueryEnd      string   `json:"query_end"`
	BaseTimeframe string   `json:"base_timeframe"`
}

//BinanceFetcher is the main worker for Binance
type BinanceFetcher struct {
	config        map[string]interface{}
	symbols       []string
	baseCurrency  string
	queryStart    time.Time
	queryEnd      time.Time
	baseTimeframe *utils.Timeframe
}

func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

func ConvertStringToFloat(str string) float64 {
	convertedString, err := strconv.ParseFloat(str, 64)
	//Store error in string array which will be checked in main fucntion later to see if there is a need to exit
	if err != nil {
		glog.Errorf("String to float error: %v", err)
		errorsConversion = append(errorsConversion, err)
	}
	return convertedString
}

//Checks time string and returns correct time format
func QueryTime(query string) time.Time {
	trials := []string{
		"2006-01-02 03:04:05",
		"2006-01-02T03:04:05",
		"2006-01-02 03:04",
		"2006-01-02T03:04",
		"2006-01-02",
	}
	for _, layout := range trials {
		qs, err := time.Parse(layout, query)
		if err == nil {
			//Returns time in correct time.Time object once it matches correct time format
			return qs.In(utils.InstanceConfig.Timezone)
		}
	}
	//Return null if no time matches time format
	return time.Time{}
}

//Convert time from milliseconds to Unix
func ConvertMillToTime(originalTime int64) time.Time {
	i := time.Unix(0, originalTime*int64(time.Millisecond))
	return i
}

// Append if String is Missing from array
// All credit to Sonia: https://stackoverflow.com/questions/9251234/go-append-if-unique
func AppendIfMissing(slice []string, i string) ([]string, bool) {
	for _, ele := range slice {
		if ele == i {
			return slice, false
		}
	}
	return append(slice, i), true
}

//Gets all symbols from binance
func GetAllSymbols() []string {
	client := binance.NewClient("", "")
	exchangeinfo, err := client.NewExchangeInfoService().Do(context.Background())
	symbol := make([]string, 0)
	status := make([]string, 0)
	validSymbols := make([]string, 0)
	notRepeated := true

	if err != nil {
		symbols := []string{"BTC", "EOS", "ETH", "BNB", "TRX", "ONT", "XRP", "ADA",
			"LTC", "BCC", "TUSD", "IOTA", "ETC", "ICX", "NEO", "XLM", "QTUM", "BCH"}
		return symbols
	} else {
		for _, info := range exchangeinfo.Symbols {
			symbol, notRepeated = AppendIfMissing(symbol, info.baseAsset)
			if notRepeated == true {
				status = append(status, info.Status)
			}
		}

		//Check status and append to symbols list if valid
		for index, s := range status {
			if s == "TRADING" {
				validSymbols = append(validSymbols, symbol[index])
			}
		}
	}

	return validSymbols
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

//Register new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	var queryEnd time.Time
	timeframeStr := "1Min"
	var symbols []string
	baseCurrency := "USDT"

	//First see if config has symbols, if not retrieve all from binance as default
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	} else {
		symbols = GetAllSymbols()
	}

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.BaseCurrency != "" {
		baseCurrency = config.BaseCurrency
	}

	if config.QueryStart != "" {
		queryStart = QueryTime(config.QueryStart)
	}

	if config.QueryEnd != "" {
		queryEnd = QueryTime(config.QueryEnd)
	}

	return &BinanceFetcher{
		config:        conf,
		baseCurrency:  baseCurrency,
		symbols:       symbols,
		queryStart:    queryStart,
		queryEnd:      queryEnd,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

//Grab data in hour intervals from starting time to ending time
func (bn *BinanceFetcher) Run() {
	symbols := bn.symbols
	client := binance.NewClient("", "")
	timeStart := time.Time{}
	finalTime := bn.queryEnd
	baseCurrency := bn.baseCurrency

	originalInterval := bn.baseTimeframe.String
	re := regexp.MustCompile("[0-9]+")
	re2 := regexp.MustCompile("[a-zA-Z]+")

	timeIntervalLettersOnly := re.ReplaceAllString(originalInterval, "")
	timeIntervalNumsOnly := re2.ReplaceAllString(originalInterval, "")

	correctIntervalSymbol := suffixBinanceDefs[timeIntervalLettersOnly]

	//If Interval is formmatted incorrectly
	if len(correctIntervalSymbol) <= 0 {
		glog.Errorf("Interval Symbol Format Incorrect. Setting to time interval to default '1Min'")
		correctIntervalSymbol = "1Min"
	}

	//Time end check
	if finalTime.IsZero() {
		finalTime = time.Now().UTC()
	}

	//Replace interval string with correct one with API call
	timeInterval := timeIntervalNumsOnly + correctIntervalSymbol

	for _, symbol := range symbols {
		tbk := io.NewTimeBucketKey(symbol + "/" + bn.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(symbol, tbk)
		glog.Infof("lastTimestamp for %s = %v", symbol, lastTimestamp)
		if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
			timeStart = lastTimestamp
		}
	}

	for {
		if timeStart.IsZero() {
			if !bn.queryStart.IsZero() {
				timeStart = bn.queryStart
			} else {
				timeStart = time.Now().UTC().Add(-time.Hour)
			}
		} else {
			timeStart = timeStart.Add(bn.baseTimeframe.Duration * 300)
		}

		timeEnd := timeStart.Add(bn.baseTimeframe.Duration * 300)

		diffTimes := finalTime.Sub(timeEnd)

		//Reset time. Make sure you get all data possible
		if diffTimes < 0 {
			timeStart = timeStart.Add(-bn.baseTimeframe.Duration * 300)
			timeEnd = finalTime
		}

		if diffTimes == 0 {
			glog.Infof("Got all data from: %v to %v", bn.queryStart, bn.queryEnd)
			glog.Infof("Continuing...")
		}

		var timeStartM int64
		var timeEndM int64

		timeStartM = timeStart.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		timeEndM = timeEnd.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

		for _, symbol := range symbols {
			glog.Infof("Requesting %s %v - %v", symbol, timeStart, timeEnd)

			rates, err := client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(timeInterval).StartTime(timeStartM).EndTime(timeEndM).Do(context.Background())

			if err != nil {
				glog.Errorf("Response error: %v", err)
				time.Sleep(time.Minute)
				continue
			}
			if len(rates) == 0 {
				glog.Info("len(rates) == 0")
				continue
			}

			openTime := make([]int64, 0)
			open := make([]float64, 0)
			high := make([]float64, 0)
			low := make([]float64, 0)
			close := make([]float64, 0)
			volume := make([]float64, 0)

			for _, rate := range rates {
				errorsConversion = errorsConversion[:0]
				openTime = append(openTime, ConvertMillToTime(rate.OpenTime).Unix())
				open = append(open, ConvertStringToFloat(rate.Open))
				high = append(high, ConvertStringToFloat(rate.High))
				low = append(low, ConvertStringToFloat(rate.Low))
				close = append(close, ConvertStringToFloat(rate.Close))
				volume = append(volume, ConvertStringToFloat(rate.Volume))

				for _, e := range errorsConversion {
					if e != nil {
						return
					}
				}
			}

			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", openTime)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			// glog.Infof("%s: %d rates between %v - %v", symbol, len(rates),
			// 	timeStart.String(), timeEnd.String())
			csm := io.NewColumnSeriesMap()
			tbk := io.NewTimeBucketKey(symbol + "/" + bn.baseTimeframe.String + "/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, false)
		}

		//Sleep for a second before next call
		time.Sleep(time.Second)
	}
}

func main() {
	symbol := "BTC"
	interval := "1m"
	baseCurrency := "USDT"

	client := binance.NewClient("", "")
	klines, err := client.NewKlinesService().Symbol(symbol + baseCurrency).
		Interval(interval).Do(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, k := range klines {
		fmt.Println(k)
	}
}
