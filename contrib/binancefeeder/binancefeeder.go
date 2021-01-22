package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"time"

	binance "github.com/adshao/go-binance"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var suffixBinanceDefs = map[string]string{
	"Min": "m",
	"H":   "h",
	"D":   "d",
	"W":   "w",
}

// ExchangeInfo exchange info
type ExchangeInfo struct {
	Symbols []struct {
		Symbol     string `json:"symbol"`
		Status     string `json:"status"`
		BaseAsset  string `json:"baseAsset"`
		QuoteAsset string `json:"quoteAsset"`
	} `json:"symbols"`
}

// getJSON via http request and decodes it using NewDecoder. Sets target interface to decoded json
func getJSON(url string, target interface{}) error {
	var myClient = &http.Client{Timeout: 10 * time.Second}
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

// For ConvertStringToFloat function and Run() function to making exiting easier
var errorsConversion []error

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	Symbols        []string `json:"symbols"`
	BaseCurrencies []string `json:"base_currencies"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// BinanceFetcher is the main worker for Binance
type BinanceFetcher struct {
	config         map[string]interface{}
	symbols        []string
	baseCurrencies []string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
}

// recast changes parsed JSON-encoded data represented as an interface to FetcherConfig structure
func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)

	return &ret
}

//Convert string to float64 using strconv
func convertStringToFloat(str string) float64 {
	convertedString, err := strconv.ParseFloat(str, 64)
	//Store error in string array which will be checked in main fucntion later to see if there is a need to exit
	if err != nil {
		log.Error("String to float error: %v", err)
		errorsConversion = append(errorsConversion, err)
	}
	return convertedString
}

//Checks time string and returns correct time format
func queryTime(query string) time.Time {
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
func convertMillToTime(originalTime int64) time.Time {
	i := time.Unix(0, originalTime*int64(time.Millisecond))
	return i
}

// Append if String is Missing from array
// All credit to Sonia: https://stackoverflow.com/questions/9251234/go-append-if-unique
func appendIfMissing(slice []string, s string) ([]string, bool) {
	for _, ele := range slice {
		if ele == s {
			return slice, false
		}
	}
	return append(slice, s), true
}

//Gets all symbols from binance
func getAllSymbols(quoteAssets []string) []string {
	symbol := make([]string, 0)
	status := make([]string, 0)
	validSymbols := make([]string, 0)
	tradingSymbols := make([]string, 0)
	quote := ""

	m := ExchangeInfo{}
	err := getJSON("https://api.binance.com/api/v1/exchangeInfo", &m)
	if err != nil {
		log.Error("Binance /exchangeInfo API error: %v", err)
		tradingSymbols = []string{"BTC", "ETH", "LTC", "BNB"}
	} else {
		for _, info := range m.Symbols {
			quote = info.QuoteAsset
			notRepeated := true
			// Check if data is the right base currency and then check if it's already recorded
			for _, quoteAsset := range quoteAssets {
				if quote == quoteAsset {
					symbol, notRepeated = appendIfMissing(symbol, info.BaseAsset)
					if notRepeated {
						status = append(status, info.Status)
					}
				}
			}
		}

		//Check status and append to symbols list if valid
		for index, s := range status {
			if s == "TRADING" {
				tradingSymbols = append(tradingSymbols, symbol[index])
			}
		}
	}

	client := binance.NewClient("", "")
	// Double check each symbol is working as intended
	for _, s := range tradingSymbols {
		_, err := client.NewKlinesService().Symbol(s + quoteAssets[0]).Interval("1m").Do(context.Background())
		if err == nil {
			validSymbols = append(validSymbols, s)
		}
	}

	return validSymbols
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
	reader, err := executor.NewReader(parsed,
		utils.InstanceConfig.DisableVariableCompression,
		utils.InstanceConfig.EnableLastKnown,
	)
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts, err := cs.GetTime()
	return ts[0]
}

// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string
	baseCurrencies := []string{"USDT"}

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	//First see if config has symbols, if not retrieve all from binance as default
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	} else {
		symbols = getAllSymbols(baseCurrencies)
	}

	if len(config.BaseCurrencies) > 0 {
		baseCurrencies = config.BaseCurrencies
	}

	return &BinanceFetcher{
		config:         conf,
		baseCurrencies: baseCurrencies,
		symbols:        symbols,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (bn *BinanceFetcher) Run() {
	symbols := bn.symbols
	client := binance.NewClient("", "")
	timeStart := time.Time{}
	baseCurrencies := bn.baseCurrencies
	slowDown := false

	// Get correct Time Interval for Binance
	originalInterval := bn.baseTimeframe.String
	re := regexp.MustCompile("[0-9]+")
	re2 := regexp.MustCompile("[a-zA-Z]+")
	timeIntervalLettersOnly := re.ReplaceAllString(originalInterval, "")
	timeIntervalNumsOnly := re2.ReplaceAllString(originalInterval, "")
	correctIntervalSymbol := suffixBinanceDefs[timeIntervalLettersOnly]
	if len(correctIntervalSymbol) <= 0 {
		log.Warn("Interval Symbol Format Incorrect. Setting to time interval to default '1Min'")
		correctIntervalSymbol = "1Min"
	}
	timeInterval := timeIntervalNumsOnly + correctIntervalSymbol

	// Get last timestamp collected
	for _, symbol := range symbols {
		for _, baseCurrency := range baseCurrencies {
			symbolDir := fmt.Sprintf("binance_%s-%s", symbol, baseCurrency)
			tbk := io.NewTimeBucketKey(symbolDir + "/" + bn.baseTimeframe.String + "/OHLCV")
			lastTimestamp := findLastTimestamp(tbk)
			log.Info("lastTimestamp for %s = %v", symbolDir, lastTimestamp)
			if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
				timeStart = lastTimestamp
			}
		}
	}

	// Set start time if not given.
	if !bn.queryStart.IsZero() {
		timeStart = bn.queryStart
	} else {
		timeStart = time.Now().UTC().Add(-bn.baseTimeframe.Duration)
	}

	// For loop for collecting candlestick data forever
	// Note that the max amount is 1000 candlesticks which is no problem
	var timeStartM int64
	var timeEndM int64
	var timeEnd time.Time
	var originalTimeStart time.Time
	var originalTimeEnd time.Time
	var originalTimeEndZero time.Time
	var waitTill time.Time
	firstLoop := true

	for {
		// finalTime = time.Now().UTC()
		originalTimeStart = timeStart
		originalTimeEnd = timeEnd

		// Check if it's finished backfilling. If not, just do 300 * Timeframe.duration
		// only do beyond 1st loop
		if !slowDown {
			if !firstLoop {
				timeStart = timeStart.Add(bn.baseTimeframe.Duration * 300)
				timeEnd = timeStart.Add(bn.baseTimeframe.Duration * 300)
			} else {
				firstLoop = false
				// Keep timeStart as original value
				timeEnd = timeStart.Add(bn.baseTimeframe.Duration * 300)
			}
			if timeEnd.After(time.Now().UTC()) {
				slowDown = true
			}
		} else {
			// Set to the :00 of previous TimeEnd to ensure that the complete candle that was not formed before is written
			originalTimeEnd = originalTimeEndZero
		}

		// Sleep for the timeframe
		// Otherwise continue to call every second to backfill the data
		// Slow Down for 1 Duration period
		// Make sure last candle is formed
		if slowDown {
			timeEnd = time.Now().UTC()
			timeStart = originalTimeEnd

			year := timeEnd.Year()
			month := timeEnd.Month()
			day := timeEnd.Day()
			hour := timeEnd.Hour()
			minute := timeEnd.Minute()

			// To prevent gaps (ex: querying between 1:31 PM and 2:32 PM (hourly)would not be ideal)
			// But we still want to wait 1 candle afterwards (ex: 1:01 PM (hourly))
			// If it is like 1:59 PM, the first wait sleep time will be 1:59, but afterwards would be 1 hour.
			// Main goal is to ensure it runs every 1 <time duration> at :00
			switch originalInterval {
			case "1Min":
				timeEnd = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
			case "1H":
				timeEnd = time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
			case "1D":
				timeEnd = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
			default:
				log.Warn("Incorrect format: %v", originalInterval)
			}
			waitTill = timeEnd.Add(bn.baseTimeframe.Duration)

			timeStartM := timeStart.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
			timeEndM := timeEnd.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

			// Make sure you get the last candle within the timeframe.
			// If the next candle is in the API call, that means the previous candle has been fully formed
			// (ex: if we see :00 is formed that means the :59 candle is fully formed)
			gotCandle := false
			for !gotCandle {
				rates, err := client.NewKlinesService().Symbol(symbols[0] + baseCurrencies[0]).Interval(timeInterval).StartTime(timeStartM).Do(context.Background())
				if err != nil {
					log.Info("Response error: %v", err)
					time.Sleep(time.Minute)
				}

				if len(rates) > 0 && rates[len(rates)-1].OpenTime-timeEndM >= 0 {
					gotCandle = true
				}
			}

			originalTimeEndZero = timeEnd
			// Change timeEnd to the correct time where the last candle is formed
			timeEnd = time.Now().UTC()
		}

		// Repeat since slowDown loop won't run if it hasn't been past the current time
		timeStartM = timeStart.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		timeEndM = timeEnd.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

		for _, symbol := range symbols {
			for _, baseCurrency := range baseCurrencies {
				log.Info("Requesting %s %v - %v", symbol, timeStart, timeEnd)
				rates, err := client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(timeInterval).StartTime(timeStartM).EndTime(timeEndM).Do(context.Background())
				if err != nil {
					log.Info("Response error: %v", err)
					log.Info("Problematic symbol %s", symbol)
					time.Sleep(time.Minute)
					// Go back to last time
					timeStart = originalTimeStart
					continue
				}
				// if len(rates) == 0 {
				// 	fmt.Printf("len(rates) == 0\n")
				// 	continue
				// }

				openTime := make([]int64, 0)
				open := make([]float64, 0)
				high := make([]float64, 0)
				low := make([]float64, 0)
				close := make([]float64, 0)
				volume := make([]float64, 0)

				for _, rate := range rates {
					errorsConversion = errorsConversion[:0]
					// if nil, do not append to list
					if rate.OpenTime != 0 && rate.Open != "" &&
						rate.High != "" && rate.Low != "" &&
						rate.Close != "" && rate.Volume != "" {
						openTime = append(openTime, convertMillToTime(rate.OpenTime).Unix())
						open = append(open, convertStringToFloat(rate.Open))
						high = append(high, convertStringToFloat(rate.High))
						low = append(low, convertStringToFloat(rate.Low))
						close = append(close, convertStringToFloat(rate.Close))
						volume = append(volume, convertStringToFloat(rate.Volume))
						for _, e := range errorsConversion {
							if e != nil {
								return
							}
						}
					} else {
						log.Info("No value in rate %v", rate)
					}
				}

				validWriting := true
				if len(openTime) == 0 || len(open) == 0 || len(high) == 0 || len(low) == 0 || len(close) == 0 || len(volume) == 0 {
					validWriting = false
				}
				// if data is nil, do not write to csm
				if validWriting {
					cs := io.NewColumnSeries()
					// Remove last incomplete candle if it exists since that is incomplete
					// Since all are the same length we can just check one
					// We know that the last one on the list is the incomplete candle because in
					// the gotCandle loop we only move on when the incomplete candle appears which is the last entry from the API
					if slowDown && len(openTime) > 1 {
						openTime = openTime[:len(openTime)-1]
						open = open[:len(open)-1]
						high = high[:len(high)-1]
						low = low[:len(low)-1]
						close = close[:len(close)-1]
						volume = volume[:len(volume)-1]
					}
					cs.AddColumn("Epoch", openTime)
					cs.AddColumn("Open", open)
					cs.AddColumn("High", high)
					cs.AddColumn("Low", low)
					cs.AddColumn("Close", close)
					cs.AddColumn("Volume", volume)
					csm := io.NewColumnSeriesMap()
					symbolDir := fmt.Sprintf("binance_%s-%s", symbol, baseCurrency)
					tbk := io.NewTimeBucketKey(symbolDir + "/" + bn.baseTimeframe.String + "/OHLCV")
					csm.AddColumnSeries(*tbk, cs)
					executor.WriteCSM(csm, false)
				}
			}
		}

		if slowDown {
			// Sleep till next :00 time
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			// Binance rate limit is 20 reequests per second so this shouldn't be an issue.
			time.Sleep(time.Second)
		}

	}
}

func main() {
	// symbol := "BTC"
	// interval := "1m"
	// baseCurrency := "USDT"

	// client := binance.NewClient("", "")
	// klines, err := client.NewKlinesService().Symbol(symbol + baseCurrency).
	// 	Interval(interval).Do(context.Background())
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// for _, k := range klines {
	// 	fmt.Println(k)
	// }
	// symbols := getAllSymbols("USDT")
	// for _, s := range symbols {
	// 	fmt.Println(s)
	// }
}
