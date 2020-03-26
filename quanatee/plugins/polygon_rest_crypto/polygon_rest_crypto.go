package main

import (
	"encoding/json"
	"fmt"
	"math"
    "math/rand"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
)


const (
    baseURL     = "https://api.polygon.io"
	aggURL      = "%v/v1/historic/agg/%v/%v"
	aggv2URL    = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	tradesURL   = "%v/v1/historic/trades/%v/%v"
	quotesURL   = "%v/v1/historic/quotes/%v/%v"
	exchangeURL = "%v/v1/meta/exchanges"
)

// AggTick is the structure that contains the actual
// tick data included in a HistoricAggregates response
type AggTick struct {
	Open              float64 `json:"o"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	Close             float64 `json:"c"`
	Volume            float64 `json:"v"`
	EpochMillisecond int64   `json:"t"`
	Items             int64   `json:"n"` // v2 response only
}
// AggType used in the HistoricAggregates response
type AggType string

const (
	// Minute timeframe aggregates
	Minute AggType = "minute"
	// Day timeframe aggregates
	Day AggType = "day"
)

// HistoricAggregates is the structure that defines
// aggregate data served through Polygon's v1 REST API.
type HistoricAggregates struct {
	Symbol        string  `json:"symbol"`
	AggregateType AggType `json:"aggType"`
	Map           struct {
		O string `json:"o"`
		C string `json:"c"`
		H string `json:"h"`
		L string `json:"l"`
		V string `json:"v"`
		D string `json:"d"`
	} `json:"map"`
	Ticks []AggTick `json:"ticks"`
}

// HistoricAggregatesV2 is the structure that defines
// aggregate data served through Polygon's v2 REST API.
type HistoricAggregatesV2 struct {
	Symbol       string    `json:"ticker"`
	Adjusted     bool      `json:"adjusted"`
	QueryCount   int       `json:"queryCount"`
	ResultsCount int       `json:"resultsCount"`
	Ticks        []AggTick `json:"results"`
}

// GetHistoricAggregates requests Polygon's v1 REST API for historic aggregates
// for the provided resolution based on the provided query parameters.
func GetHistoricAggregates(
    api_key string,
	symbol string,
	from, to *time.Time,
	limit *int) (*HistoricAggregates, error) {

	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, Minute, symbol))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", api_key)

	if from != nil {
		q.Set("from", from.Format(time.RFC3339))
	}

	if to != nil {
		q.Set("to", to.Format(time.RFC3339))
	}

	if limit != nil {
		q.Set("limit", strconv.FormatInt(int64(*limit), 10))
	}

	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", resp.StatusCode)
	}

	agg := &HistoricAggregates{}

	if err = unmarshal(resp, agg); err != nil {
		return nil, err
	}

	return agg, nil
}

// GetHistoricAggregates requests Polygon's v2 REST API for historic aggregates
// for the provided resolution based on the provided query parameters.
func GetHistoricAggregatesV2(
    api_key string,
	symbol string,
	multiplier int,
	from, to *time.Time,
	unadjusted *bool) (*HistoricAggregatesV2, error) {
    
	u, err := url.Parse(fmt.Sprintf(aggv2URL, baseURL, symbol, multiplier, Minute, from.Unix()*1000, to.Unix()*1000))
    
    log.Info("GetHistoricAggregatesV2 %s", u)
	
    if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", api_key)

	if unadjusted != nil {
		q.Set("unadjusted", strconv.FormatBool(*unadjusted))
	}

	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", resp.StatusCode)
	}

	agg := &HistoricAggregatesV2{}

	if err = unmarshal(resp, agg); err != nil {
		return nil, err
	}

	return agg, nil
}

func unmarshal(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}
var suffixPolygonCryptoDefs = map[string]string{
	"Min": "minute",
	"H":   "hour",
	"D":   "day",
	"W":   "week",
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
    ApiKey         string   `json:"api_key"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// PolygonCryptoFetcher is the main worker for PolygonCrypto
type PolygonCryptoFetcher struct {
	config         map[string]interface{}
	symbols        []string
	baseCurrencies []string
    apiKey         string
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

//Convert time from Millisecond to Unix
func convertMillToTime(originalTime int64) time.Time {
	i := time.Unix(0, originalTime*int64(time.Millisecond))
	return i
}

func findLastTimestamp(tbk *io.TimeBucketKey) time.Time {
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
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts := cs.GetTime()
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

	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}

	if len(config.BaseCurrencies) > 0 {
		baseCurrencies = config.BaseCurrencies
	}

	return &PolygonCryptoFetcher{
		config:         conf,
		baseCurrencies: baseCurrencies,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (pgc *PolygonCryptoFetcher) Run() {
    
    var rateLimit, rateCount int = 7, 0

	symbols := pgc.symbols
    
	timeStart := time.Time{}
	baseCurrencies := pgc.baseCurrencies
	slowDown := false
    
	// Get correct Time Interval for PolygonCrypto
	originalInterval := pgc.baseTimeframe.String
	re := regexp.MustCompile("[0-9]+")
	re2 := regexp.MustCompile("[a-zA-Z]+")
	timeIntervalLettersOnly := re.ReplaceAllString(originalInterval, "")
	timeIntervalNumsOnly := re2.ReplaceAllString(originalInterval, "")
    timeIntervalNumsOnlyInt, err := strconv.Atoi(timeIntervalNumsOnly)
    if err != nil {
        log.Info("PolygonCrypto strconv.Atoi(timeIntervalNumsOnly): %s", err)
    }
	correctIntervalSymbol := suffixPolygonCryptoDefs[timeIntervalLettersOnly]
	if len(correctIntervalSymbol) <= 0 {
		log.Warn("Interval Symbol Format Incorrect. Setting to time interval to default '1Min'")
		correctIntervalSymbol = "1Min"
	}
	// timeInterval := timeIntervalNumsOnly + correctIntervalSymbol
    
	// Get last timestamp collected
	for _, symbol := range symbols {
		for _, baseCurrency := range baseCurrencies {
			symbolDir := fmt.Sprintf("%s-%s", symbol, baseCurrency)
			tbk := io.NewTimeBucketKey(symbolDir + "/" + pgc.baseTimeframe.String + "/OHLCV")
			lastTimestamp := findLastTimestamp(tbk)
			log.Info("PolygonCrypto: lastTimestamp for %s = %v", symbolDir, lastTimestamp)
			if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
				timeStart = lastTimestamp.UTC()
			}
		}
	}
    
	// Set start time if not given.
	if !pgc.queryStart.IsZero() {
		timeStart = pgc.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC().Add(-pgc.baseTimeframe.Duration)
	}

	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if !firstLoop {
            if !slowDown {
                // If next batch of backfill goes into the future, switch to slowDown (realtime)
                if timeEnd.Add(pgc.baseTimeframe.Duration * 150).After(time.Now().UTC()) {
                    // Set slowdown; starts requests start of new timeframe
                    // Also purposefully burst rateLimit to force delay one time
                    rateCount = 99
                    slowDown = true
                    timeStart = timeEnd
                    timeEnd = time.Now().UTC()
                // If still backfilling
                } else {
                    timeStart = timeEnd
                    timeEnd = timeEnd.Add(pgc.baseTimeframe.Duration * 150)
                }
            // if slowDown (realtime)
            } else {
                timeStart = timeEnd
                timeEnd = time.Now().UTC()
            }
        // firstLoop, we use this if we get timed out as well
        } else {
            firstLoop = false
            // Keep timeStart as original value
            timeEnd = timeStart.Add(pgc.baseTimeframe.Duration * 150)            
        }
        
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
            log.Warn("PolygonCrypto: Incorrect format: %v", originalInterval)
        }
        
        // Shuffle symbol sequence so no symbol has priority
        rand.Shuffle(len(symbols), func(i, j int) {
            symbols[i], symbols[j] = symbols[j], symbols[i]
        })
		for _, symbol := range symbols {
			for _, baseCurrency := range baseCurrencies {
                rateCount = rateCount + 1
                if rateCount >= rateLimit {
                    time.Sleep(time.Second)
                    rateCount = 0
                }
                
				epoch := make([]int64, 0)
				open := make([]float64, 0)
				high := make([]float64, 0)
				low := make([]float64, 0)
				close := make([]float64, 0)
				volume := make([]float64, 0)

                log.Info("PolygonCrypto: Requesting %s-%s %v - %v", symbol, baseCurrency, timeStart, timeEnd)                
                unadjusted := false
                rates, err := GetHistoricAggregatesV2(pgc.apiKey, symbol + "-" + baseCurrency, timeIntervalNumsOnlyInt, &timeStart, &timeEnd, &unadjusted)
                
                if err != nil {
					log.Info("PolygonCrypto: %s-%s Response error: %v", symbol, baseCurrency, err)
					time.Sleep(time.Millisecond*time.Duration(rand.Intn(1000)))
                    // Error at request level
                    if slowDown {
                        // slowDown means running in realtime, important to not exceed ratelimit
                        // we assume exchange was down, zero the data for aggregator to detect anomaly instead of waiting forever
                        epoch = append(epoch, timeEnd.Unix())
                        open = append(open, 0)
                        high = append(high, 0)
                        low = append(low, 0)
                        close = append(close, 0)
                        volume = append(volume, 0)
                    } else {
                        // retries downloading the same time period again by resetting firstLoop bool
                        firstLoop = true
                        continue
                    }
				} else {
                    // process downloaded rates
                    rates_err := false
                    if len(rates.Ticks) == 0 {
                        log.Info("PolygonCrypto: Exchange has no data from: %s-%s %v-%v", symbol, baseCurrency, timeStart, timeEnd)
                        rates_err = true
                    } else {
                        for _, rate := range rates.Ticks {
                            log.Info("%v %v %v %v %v %v", rate.EpochMillisecond, rate.Open, rate.High, rate.Low, rate.Close, rate.Volume)
                            if rate.EpochMillisecond != 0 && rate.Open != 0 &&
                                rate.High != 0 && rate.Low != 0 &&
                                rate.Close != 0 && rate.Volume != 0 {
                                epoch = append(epoch, rate.EpochMillisecond/1000)
                                open = append(open, rate.Open)
                                high = append(high, rate.High)
                                low = append(low, rate.Low)
                                close = append(close, rate.Close)
                                volume = append(volume, rate.Volume)                                
                            } else {
                                log.Info("PolygonCrypto: Downloaded OHLCV contained 0 from: %s-%s %v-%v", symbol, baseCurrency, timeStart, timeEnd)
                                rates_err = true
                                break
                            }
                        }
                    }                    
                    if rates_err {
                        // If data appears corrupted (most likely data simply does not exist)
                        // In this event, we rubbish the entire range of rates and write zero'ed data
                        // This is not an issue for realtime since only 1 rate will be zero'ed- though why the error occured at the exchange is an issue
                        epoch = make([]int64, 0)
                        open = make([]float64, 0)
                        high = make([]float64, 0)
                        low = make([]float64, 0)
                        close = make([]float64, 0)
                        volume = make([]float64, 0)
                        for t := timeStart.Unix(); t <= timeEnd.Unix(); t=t+int64(pgc.baseTimeframe.Duration.Seconds()) {
                            epoch = append(epoch, t)
                            open = append(open, 0)
                            high = append(high, 0)
                            low = append(low, 0)
                            close = append(close, 0)
                            volume = append(volume, 0)
                        }
                    }
                }
                
                // write to csm
                cs := io.NewColumnSeries()
                cs.AddColumn("Epoch", epoch)
                cs.AddColumn("Open", open)
                cs.AddColumn("High", high)
                cs.AddColumn("Low", low)
                cs.AddColumn("Close", close)
                cs.AddColumn("Volume", volume)
                csm := io.NewColumnSeriesMap()
                symbolDir := fmt.Sprintf("%s-%s", symbol, baseCurrency)
                tbk := io.NewTimeBucketKey(symbolDir + "/" + pgc.baseTimeframe.String + "/OHLCV")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
			}
		}

		if slowDown {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(pgc.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            log.Info("PolygonCrypto: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
            rateCount = 0
		} else {
			time.Sleep(time.Second*7)
		}

	}
}

func main() {
}
