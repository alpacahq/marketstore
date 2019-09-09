package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    "strconv"
    "strings"
    "math/rand"
    "math/big"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
	"gopkg.in/yaml.v2"
	"github.com/alpacahq/marketstore/quanatee/plugins/quanatee_forex_prices/calendar"
)

// Quote - stucture for historical price data
type Quote struct {
	Symbol    string      `json:"symbol"`
	Precision int64       `json:"-"`
	Epoch     []int64     `json:"epoch"`
	Open      []float64   `json:"open"`
	High      []float64   `json:"high"`
	Low       []float64   `json:"low"`
	Close     []float64   `json:"close"`
	Volume    []float64   `json:"volume"`
}

// Quotes - an array of historical price data
type Quotes []Quote

// ClientTimeout - connect/read timeout for client requests
const ClientTimeout = 15 * time.Second

// NewQuote - new empty Quote struct
func NewQuote(symbol string, bars int) Quote {
	return Quote{
		Symbol: symbol,
		Epoch:  make([]int64,   bars),
		Open:   make([]float64, bars),
		High:   make([]float64, bars),
		Low:    make([]float64, bars),
		Close:  make([]float64, bars),
		Volume: make([]float64, bars),
	}
}

func GetIntrinioPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, calendar *cal.Calendar, token string) (Quote, error) {
    
	resampleFreq := "H1"
	switch period.String {
	case "1Min":
		resampleFreq = "m1"
	case "5Min":
		resampleFreq = "m5"
	case "15Min":
		resampleFreq = "m15"
	case "30Min":
		resampleFreq = "m30"
	case "1H":
		resampleFreq = "H1"
	case "2H":
		resampleFreq = "H2"
	case "4H":
		resampleFreq = "H4"
	case "6H":
		resampleFreq = "H6"
	case "8H":
		resampleFreq = "H8"
	}

	type pairData struct {
		Symbol         string  `json:"code"`
		BaseCurrency   string  `json:"base_currency"`
		QuoteCurrency  string  `json:"quote_currency"`
	}
    
	type priceData struct {
		Date             string `json:"occurred_at"` // "2017-12-19T00:00:00Z"
		OpenBid          string `json:"open_bid"`
		HighBid          string `json:"high_bid"`
		LowBid           string `json:"low_bid"`
		CloseBid         string `json:"close_bid"`
 		OpenAsk          string `json:"open_ask"`
		HighAsk          string `json:"high_ask"`
		LowAsk           string `json:"low_ask"`
		CloseAsk         string `json:"close_ask"`
		TotalTicks       int64   `json:"total_ticks"`
	}
    
	type intrinioData struct {
		PriceData     []priceData `json:"prices"`
		PairData      pairData    `json:"pair"`
		Page          string      `json:"next_page"`
	}
    
	var forexData intrinioData

    apiUrl := fmt.Sprintf(
                        "https://api-v2.intrinio.com/forex/prices/%s/%s?api_key=%s&start_date=%s&start_time=%s",
                        symbol,
                        resampleFreq,
                        token,
                        url.QueryEscape(from.Add(-period.Duration).Format("2006-01-02")),
                        url.QueryEscape(from.Add(-period.Duration).Format("15:04:05")))
    
    if !realTime {
        apiUrl = apiUrl + "&end_date=" + url.QueryEscape(to.Format("2006-01-02")) + "&end_time=" + url.QueryEscape(to.Format("15:04:05"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", apiUrl, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := client.Do(req)
    
    // Try again if fail
	if err != nil {
        time.Sleep(1 * time.Second)    
        resp, err = client.Do(req)
    }
    
	if err != nil {
		log.Error("Forex: Intrinio symbol '%s' error: %s \n %s", symbol, err, apiUrl)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Error(": Intrinio symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData.PriceData) < 1 {
        if ( calendar.IsWorkday(from) && ( ( int(from.Weekday()) >= 1 && int(from.Weekday()) <= 4 ) || ( int(from.Weekday()) == 5 && from.Hour() < 21 ) ) ) {
            log.Error("Forex: Intrinio symbol '%s' No data returned from %v-%v, \n %s", symbol, from, to, apiUrl)
        }
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData.PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, forexData.PriceData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            open_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].OpenBid, 64) 
            open_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].OpenAsk, 64)
            high_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].HighBid, 64) 
            high_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].HighAsk, 64)
            low_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].LowBid, 64) 
            low_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].LowAsk, 64)
            close_bid, _ := strconv.ParseFloat(forexData.PriceData[bar].CloseBid, 64) 
            close_ask, _ := strconv.ParseFloat(forexData.PriceData[bar].CloseAsk, 64)
            
            quote.Open[bar] = (open_bid + open_ask) / 2
            quote.High[bar] = (high_bid + high_ask) / 2
            quote.Low[bar] = (low_bid + low_ask) / 2
            quote.Close[bar] = (close_bid + close_ask) / 2
            quote.Volume[bar] = float64(forexData.PriceData[bar].TotalTicks)
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
    // Reverse the order of slice in Intrinio because data is returned in descending (latest to earliest) whereas Tiingo does it from ascending (earliest to latest)
    for i, j := 0, len(quote.Epoch)-1; i < j; i, j = i+1, j-1 {
        quote.Epoch[i], quote.Epoch[j] = quote.Epoch[j], quote.Epoch[i]
        quote.Open[i], quote.Open[j] = quote.Open[j], quote.Open[i]
        quote.High[i], quote.High[j] = quote.High[j], quote.High[i]
        quote.Low[i], quote.Low[j] = quote.Low[j], quote.Low[i]
        quote.Close[i], quote.Close[j] = quote.Close[j], quote.Close[i]
        quote.Volume[i], quote.Volume[j] = quote.Volume[j], quote.Volume[i]
    }

	return quote, nil
}

func GetTiingoPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, calendar *cal.Calendar, token string) (Quote, error) {
    
	resampleFreq := "1hour"
	switch period.String {
	case "1Min":
		resampleFreq = "1min"
	case "5Min":
		resampleFreq = "5min"
	case "15Min":
		resampleFreq = "15min"
	case "30Min":
		resampleFreq = "30min"
	case "1H":
		resampleFreq = "1hour"
	case "2H":
		resampleFreq = "2hour"
	case "4H":
		resampleFreq = "4hour"
	case "6H":
		resampleFreq = "6hour"
	case "8H":
		resampleFreq = "8hour"
	}

	type priceData struct {
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		Ticker         string  `json:"ticker"`
		Open           float64 `json:"open"`
		Low            float64 `json:"low"`
		High           float64 `json:"high"`
		Close          float64 `json:"close"`
	}
    
	var forexData []priceData
    
    apiUrl := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/fx/%s/prices?resampleFreq=%s&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    // Pad to with an extra day if backfilling to ensure that start_date and end_date is different
    if !realTime && to.AddDate(0, 0, 1).After(time.Now().UTC()) {
        apiUrl = apiUrl + "&endDate=" + url.QueryEscape(to.AddDate(0, 0, 1).Format("2006-1-2"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", apiUrl, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)
    
    // Try again if fail
	if err != nil {
        time.Sleep(250 * time.Millisecond)
        resp, err = client.Do(req)
    }
    
	if err != nil {
		log.Error("Forex: Tiingo symbol '%s' error: %s \n %s", symbol, err, apiUrl)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Error("Forex: Tiingo symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData) < 1 {
        if ( calendar.IsWorkday(from) && ( ( int(from.Weekday()) >= 1 && int(from.Weekday()) <= 4 ) || ( int(from.Weekday()) == 5 && from.Hour() < 21 ) ) ) {
            log.Warn("Forex: Tiingo symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, apiUrl)
        }
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, forexData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = forexData[bar].Open
            quote.High[bar] = forexData[bar].High
            quote.Low[bar] = forexData[bar].Low
            quote.Close[bar] = forexData[bar].Close
            quote.Volume[bar] = 1.0
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
}

type FetcherConfig struct {
	Symbols        []string `yaml:"symbols"`
    Indices        map[string][]string `yaml:"indices"`
    ApiKey         string   `yaml:"api_key"`
    ApiKey2        string   `yaml:"api_key2"`
	QueryStart     string   `yaml:"query_start"`
	BaseTimeframe  string   `yaml:"base_timeframe"`
}

// ForexFetcher is the main worker for TiingoForex
type ForexFetcher struct {
	config         map[string]interface{}
	symbols        []string
	indices        map[string][]string
    apiKey         string
    apiKey2        string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
}

// recast changes parsed yaml-encoded data represented as an interface to FetcherConfig structure
func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := yaml.Marshal(config)
	ret := FetcherConfig{}
	yaml.Unmarshal(data, &ret)

	return &ret
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

func alignTimeToTradingHours(timeCheck time.Time, calendar *cal.Calendar) time.Time {
    
    // Forex Opening = Monday 0700 UTC is the first data we will consume in a session (London Open)
    // Forex Closing = Friday 2100 UTC is the last data we will consume in a session (New York Close)
    // In the event of a holiday, we close at 2100 UTC and open at 0700 UTC
    // We do not account for disruptions in Marketstore
    // Aligning time series datas is done in Quanatee functions

    if !calendar.IsWorkday(timeCheck) || ( !calendar.IsWorkday(timeCheck.AddDate(0, 0, 1)) && timeCheck.Hour() >= 21 ) {
        // Current date is not a Work Day, or next day is not a Work Day and current Work Day in New York has ended
        // Find the next Work Day and set to Germany Opening (Overlaps with Japan)
        nextWorkday := false
        days := 1
        for nextWorkday == false {
            if calendar.IsWorkday(timeCheck.AddDate(0, 0, days)) {
                nextWorkday = true
                break
            } else {
                days += 1
            }
        }
        timeCheck = timeCheck.AddDate(0, 0, days)
        timeCheck = time.Date(timeCheck.Year(), timeCheck.Month(), timeCheck.Day(), 7, 0, 0, 0, time.UTC)
    }
    return timeCheck
}

// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string
	var indices map[string][]string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}
    
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	} else {
        for _, value := range config.Indices {
            for _, symbol := range value {
                if !strings.Contains(symbol, "-") {
                    symbols = append(symbols, symbol)
                }
            }
        }
    }
    
	if len(config.Indices) > 0 {
		indices = config.Indices
	}
    
	return &ForexFetcher{
		config:         conf,
		symbols:        symbols,
		indices:        indices,
        apiKey:         config.ApiKey,
        apiKey2:        config.ApiKey2,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiifx *ForexFetcher) Run() {
    
	realTime := false    
	timeStart := time.Time{}
	lastTimestamp := time.Time{}
    
    // Get last timestamp collected
	for _, symbol := range tiifx.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiifx.baseTimeframe.String + "/OHLCV")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("Forex: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
    calendar := cal.NewCalendar()

    // Add US and UK holidays
    calendar.AddHoliday(
        cal.USNewYear,
        cal.USMLK,
        cal.USPresidents,
        cal.GoodFriday,
        cal.USMemorial,
        cal.USIndependence,
        cal.USLabor,
        cal.USThanksgiving,
        cal.USChristmas,
		cal.GBNewYear,
		cal.GBGoodFriday,
		cal.GBEasterMonday,
		cal.GBEarlyMay,
		cal.GBSpringHoliday,
		cal.GBSummerHoliday,
		cal.GBChristmasDay,
		cal.GBBoxingDay,
    )
    
	// Set start time if not given.
	if !tiifx.queryStart.IsZero() {
		timeStart = tiifx.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC()
	}
    timeStart = alignTimeToTradingHours(timeStart, calendar)
    
	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    dataProvider := "None"
    
	for {
        
        if firstLoop {
            firstLoop = false
        } else {
            timeStart = timeEnd
        }
        if realTime {
            // Add timeEnd by a tick
            timeEnd = timeStart.Add(tiifx.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.Add(tiifx.baseTimeframe.Duration * 99)
            if timeEnd.After(time.Now().UTC()) {
                // timeEnd is after current time
                realTime = true
                timeEnd = time.Now().UTC()
            }
        }
        
        /*
        To prevent gaps (ex: querying between 1:31 PM and 2:32 PM (hourly)would not be ideal)
        But we still want to wait 1 candle afterwards (ex: 1:01 PM (hourly))
        If it is like 1:59 PM, the first wait sleep time will be 1:59, but afterwards would be 1 hour.
        Main goal is to ensure it runs every 1 <time duration> at :00
        Tiingo returns data by the day, regardless of granularity
        */
        year := timeEnd.Year()
        month := timeEnd.Month()
        day := timeEnd.Day()
        hour := timeEnd.Hour()
        minute := timeEnd.Minute()
        timeEnd = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)

        quotes := Quotes{}
        symbols := tiifx.symbols
        rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
        // Data for symbols are retrieved in random order for fairness
        // Data for symbols are written immediately for asynchronous-like processing
        for _, symbol := range symbols {
            time.Sleep(100 * time.Millisecond)
            time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
            tiingoQuote, _ := GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiifx.baseTimeframe, calendar, tiifx.apiKey)
            intrinioQuote, _ := GetIntrinioPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiifx.baseTimeframe, calendar, tiifx.apiKey2)
            quote := NewQuote(symbol, 0)
            if len(tiingoQuote.Epoch) > 0 && len(intrinioQuote.Epoch) > 0 {
                quote = intrinioQuote
                numrows := len(tiingoQuote.Epoch)
                for bar := 0; bar < numrows; bar++ {
                    matchedEpochs := false
                    matchedBar    := bar
                    // First Test
                    if len(intrinioQuote.Epoch) > bar {
                        if tiingoQuote.Epoch[bar] == intrinioQuote.Epoch[bar] {
                            // Shallow Iteration on tiingoQuote matches with intrinioQuote
                            matchedEpochs = true
                            matchedBar = bar
                        }
                    }
                    // Second Test
                    if !matchedEpochs {
                        // Nested Iteration on intrinioQuote to match tiingoQuote with intrinioQuote
                        numrows2 := len(quote.Epoch)
                        for bar2 := 0; bar2 < numrows2; bar2++ {
                            if tiingoQuote.Epoch[bar] == quote.Epoch[bar2] {
                                matchedEpochs = true
                                matchedBar = bar2
                                break
                            }
                        }
                    }
                    if !matchedEpochs {
                        // If no Epochs were matched, it means tiingoQuote contains Epoch that intrinioQuote does not have
                        quote.Epoch = append(quote.Epoch, tiingoQuote.Epoch[bar])
                        quote.Open = append(quote.Open, tiingoQuote.Open[bar])
                        quote.High = append(quote.High, tiingoQuote.High[bar])
                        quote.Low = append(quote.Low, tiingoQuote.Low[bar])
                        quote.Close = append(quote.Close, tiingoQuote.Close[bar])
                        quote.Volume = append(quote.Volume, tiingoQuote.Volume[bar])
                    } else {
                        // Calculate the market capitalization
                        tiingoQuoteCap := new(big.Float).Mul(big.NewFloat(tiingoQuote.Close[bar]), big.NewFloat(tiingoQuote.Volume[bar]))
                        intrinioQuoteCap := new(big.Float).Mul(big.NewFloat(intrinioQuote.Close[matchedBar]), big.NewFloat(intrinioQuote.Volume[matchedBar]))
                        totalCap := new(big.Float).Add(tiingoQuoteCap, intrinioQuoteCap)
                        // Calculate the weighted averages
                        tiingoQuoteWeight := new(big.Float).Quo(tiingoQuoteCap, totalCap)
                        intrinioQuoteWeight := new(big.Float).Quo(intrinioQuoteCap, totalCap)
                        
                        weightedOpen := new(big.Float).Mul(big.NewFloat(tiingoQuote.Open[bar]), tiingoQuoteWeight)
                        weightedOpen = weightedOpen.Add(weightedOpen, new(big.Float).Mul(big.NewFloat(intrinioQuote.Open[matchedBar]), intrinioQuoteWeight))
                        
                        weightedHigh := new(big.Float).Mul(big.NewFloat(tiingoQuote.High[bar]), tiingoQuoteWeight)
                        weightedHigh = weightedHigh.Add(weightedHigh, new(big.Float).Mul(big.NewFloat(intrinioQuote.High[matchedBar]), intrinioQuoteWeight))
                        
                        weightedLow := new(big.Float).Mul(big.NewFloat(tiingoQuote.Low[bar]), tiingoQuoteWeight)
                        weightedLow = weightedLow.Add(weightedLow, new(big.Float).Mul(big.NewFloat(intrinioQuote.Low[matchedBar]), intrinioQuoteWeight))
                        
                        weightedClose := new(big.Float).Mul(big.NewFloat(tiingoQuote.Close[bar]), tiingoQuoteWeight)
                        weightedClose = weightedClose.Add(weightedClose, new(big.Float).Mul(big.NewFloat(intrinioQuote.Close[matchedBar]), intrinioQuoteWeight))
                        
                        quote.Open[matchedBar], _ = weightedOpen.Float64()
                        quote.High[matchedBar], _ = weightedHigh.Float64()
                        quote.Low[matchedBar], _ = weightedLow.Float64()
                        quote.Close[matchedBar], _ = weightedClose.Float64()
                        quote.Volume[matchedBar], _ = totalCap.Quo(totalCap, weightedClose).Float64()
                    }
                }
                dataProvider = "Aggregation"
            } else if len(tiingoQuote.Epoch) > 0 && tiingoQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = tiingoQuote
                dataProvider = "Tiingo"
            } else if len(intrinioQuote.Epoch) > 0 && intrinioQuote.Epoch[0] > 0 && intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = intrinioQuote
                dataProvider = "Intrinio"
            } else {
                dataProvider = "None"
                continue
            }
            
            if len(quote.Epoch) < 1 {
                // Check if there is data to add
                continue
            } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                // Check if realTime is adding the most recent data
                log.Warn("Forex: Previous row dated %v is still the latest in %s/%s/OHLCV", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiifx.baseTimeframe.String)
                continue
            }
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            cs.AddColumn("Volume", quote.Volume)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLCV")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
            
            // Save the latest timestamp written
            lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0)
            log.Info("Forex: %v row(s) to %s/%s/OHLCV from %v to %v by %s", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
            quotes = append(quotes, quote)
        }
        
        // Add reversed pairs
        for _, quote := range quotes {
            revSymbol := ""
            if strings.HasPrefix(quote.Symbol, "USD") {
                revSymbol = strings.Replace(quote.Symbol, "USD", "", -1) + "USD"
            } else if strings.HasPrefix(quote.Symbol, "EUR") {
                revSymbol = strings.Replace(quote.Symbol, "EUR", "", -1) + "EUR"
            } else if strings.HasPrefix(quote.Symbol, "JPY") {
                revSymbol = strings.Replace(quote.Symbol, "JPY", "", -1) + "JPY"
            }
            if revSymbol != "" {
                numrows := len(quote.Epoch)
                revQuote := NewQuote(revSymbol, numrows)
                for bar := 0; bar < numrows; bar++ {
                    revQuote.Epoch[bar] = quote.Epoch[bar]
                    revQuote.Open[bar] = 1/quote.Open[bar]
                    revQuote.High[bar] = 1/quote.High[bar]
                    revQuote.Low[bar] = 1/quote.Low[bar]
                    revQuote.Close[bar] = 1/quote.Close[bar]
                    x := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), big.NewFloat(quote.Volume[bar]))
                    z := new(big.Float).Quo(x, big.NewFloat(revQuote.Close[bar]))
                    revQuote.Volume[bar], _ = z.Float64()
                }
                // write to csm
                cs := io.NewColumnSeries()
                cs.AddColumn("Epoch", revQuote.Epoch)
                cs.AddColumn("Open", revQuote.Open)
                cs.AddColumn("High", revQuote.High)
                cs.AddColumn("Low", revQuote.Low)
                cs.AddColumn("Close", revQuote.Close)
                cs.AddColumn("Volume", revQuote.Volume)
                csm := io.NewColumnSeriesMap()
                tbk := io.NewTimeBucketKey(revQuote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLCV")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Debug("Forex: %v inverted row(s) to %s/%s/OHLCV from %v to %v", len(revQuote.Epoch), revQuote.Symbol, tiifx.baseTimeframe.String, time.Unix(revQuote.Epoch[0], 0).UTC(), time.Unix(revQuote.Epoch[len(revQuote.Epoch)-1], 0).UTC())
                quotes = append(quotes, revQuote)
            }
        }
        
        aggQuotes := Quotes{}
        for key, value := range tiifx.indices {
            aggQuote := NewQuote(key, 0)
            for _, quote := range quotes {
                for _, symbol := range value {
                    if quote.Symbol == symbol {
                        if len(quote.Epoch) > 0 {
                            if len(aggQuote.Epoch) == 0 && len(quote.Epoch) > 0 {
                                aggQuote.Epoch = quote.Epoch
                                aggQuote.Open = quote.Open
                                aggQuote.High = quote.High
                                aggQuote.Low = quote.Low
                                aggQuote.Close = quote.Close
                                aggQuote.Volume = quote.Volume
                            } else if len(aggQuote.Epoch) == len(quote.Epoch) && aggQuote.Epoch[0] == quote.Epoch[0] && aggQuote.Epoch[len(aggQuote.Epoch)-1] == quote.Epoch[len(quote.Epoch)-1] {
                                // aggQuote (Index) and quote (new symbol to be added) matches in row length and start/end points
                                numrows := len(aggQuote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    // Calculate the market capitalization
                                    quoteCap := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), big.NewFloat(quote.Volume[bar]))
                                    aggQuoteCap := new(big.Float).Mul(big.NewFloat(aggQuote.Close[bar]), big.NewFloat(aggQuote.Volume[bar]))
                                    totalCap := new(big.Float).Add(quoteCap, aggQuoteCap)
                                    // Calculate the weighted averages
                                    quoteWeight := new(big.Float).Quo(quoteCap, totalCap)
                                    aggQuoteWeight := new(big.Float).Quo(aggQuoteCap, totalCap)
                                    
                                    weightedOpen := new(big.Float).Mul(big.NewFloat(quote.Open[bar]), quoteWeight)
                                    weightedOpen = weightedOpen.Add(weightedOpen, new(big.Float).Mul(big.NewFloat(aggQuote.Open[bar]), aggQuoteWeight))
                                    
                                    weightedHigh := new(big.Float).Mul(big.NewFloat(quote.High[bar]), quoteWeight)
                                    weightedHigh = weightedHigh.Add(weightedHigh, new(big.Float).Mul(big.NewFloat(aggQuote.High[bar]), aggQuoteWeight))
                                    
                                    weightedLow := new(big.Float).Mul(big.NewFloat(quote.Low[bar]), quoteWeight)
                                    weightedLow = weightedLow.Add(weightedLow, new(big.Float).Mul(big.NewFloat(aggQuote.Low[bar]), aggQuoteWeight))
                                    
                                    weightedClose := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), quoteWeight)
                                    weightedClose = weightedClose.Add(weightedClose, new(big.Float).Mul(big.NewFloat(aggQuote.Close[bar]), aggQuoteWeight))
                                    
                                    aggQuote.Open[bar], _ = weightedOpen.Float64()
                                    aggQuote.High[bar], _ = weightedHigh.Float64()
                                    aggQuote.Low[bar], _ = weightedLow.Float64()
                                    aggQuote.Close[bar], _ = weightedClose.Float64()
                                    aggQuote.Volume[bar], _ = totalCap.Quo(totalCap, weightedClose).Float64()
                                }
                            } else if len(aggQuote.Epoch) > 0 && len(quote.Epoch) > 0 {
                                // aggQuote (Index) and quote (new symbol to be added) does not match in row length or start/end points
                                numrows := len(quote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    matchedEpochs := false
                                    matchedBar    := bar
                                    // First Test
                                    if len(aggQuote.Epoch) > bar {
                                        if quote.Epoch[bar] == aggQuote.Epoch[bar] {
                                            // Shallow Iteration on quote matches with aggQuote
                                            matchedEpochs = true
                                            matchedBar = bar
                                        }
                                    }
                                    // Second Test
                                    if !matchedEpochs {
                                        // Nested Iteration on aggQuote to match quote with aggQuote
                                        numrows2 := len(aggQuote.Epoch)
                                        for bar2 := 0; bar2 < numrows2; bar2++ {
                                            if quote.Epoch[bar] == aggQuote.Epoch[bar2] {
                                                matchedEpochs = true
                                                matchedBar = bar2
                                                break
                                            }
                                        }
                                    }
                                    if !matchedEpochs {
                                        // If no Epochs were matched, it means quote contains Epoch that aggQuote does not have
                                        aggQuote.Epoch = append(aggQuote.Epoch, quote.Epoch[bar])
                                        aggQuote.Open = append(aggQuote.Open, quote.Open[bar])
                                        aggQuote.High = append(aggQuote.High, quote.High[bar])
                                        aggQuote.Low = append(aggQuote.Low, quote.Low[bar])
                                        aggQuote.Close = append(aggQuote.Close, quote.Close[bar])
                                        aggQuote.Volume = append(aggQuote.Volume, quote.Volume[bar])
                                    } else {
                                        // Calculate the market capitalization
                                        quoteCap := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), big.NewFloat(quote.Volume[bar]))
                                        aggQuoteCap := new(big.Float).Mul(big.NewFloat(aggQuote.Close[matchedBar]), big.NewFloat(aggQuote.Volume[matchedBar]))
                                        totalCap := new(big.Float).Add(quoteCap, aggQuoteCap)
                                        // Calculate the weighted averages
                                        quoteWeight := new(big.Float).Quo(quoteCap, totalCap)
                                        aggQuoteWeight := new(big.Float).Quo(aggQuoteCap, totalCap)
                                        
                                        weightedOpen := new(big.Float).Mul(big.NewFloat(quote.Open[bar]), quoteWeight)
                                        weightedOpen = weightedOpen.Add(weightedOpen, new(big.Float).Mul(big.NewFloat(aggQuote.Open[matchedBar]), aggQuoteWeight))
                                        
                                        weightedHigh := new(big.Float).Mul(big.NewFloat(quote.High[bar]), quoteWeight)
                                        weightedHigh = weightedHigh.Add(weightedHigh, new(big.Float).Mul(big.NewFloat(aggQuote.High[matchedBar]), aggQuoteWeight))
                                        
                                        weightedLow := new(big.Float).Mul(big.NewFloat(quote.Low[bar]), quoteWeight)
                                        weightedLow = weightedLow.Add(weightedLow, new(big.Float).Mul(big.NewFloat(aggQuote.Low[matchedBar]), aggQuoteWeight))
                                        
                                        weightedClose := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), quoteWeight)
                                        weightedClose = weightedClose.Add(weightedClose, new(big.Float).Mul(big.NewFloat(aggQuote.Close[matchedBar]), aggQuoteWeight))
                                        
                                        aggQuote.Open[matchedBar], _ = weightedOpen.Float64()
                                        aggQuote.High[matchedBar], _ = weightedHigh.Float64()
                                        aggQuote.Low[matchedBar], _ = weightedLow.Float64()
                                        aggQuote.Close[matchedBar], _ = weightedClose.Float64()
                                        aggQuote.Volume[matchedBar], _ = totalCap.Quo(totalCap, weightedClose).Float64()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if len(aggQuote.Epoch) > 0 {
                aggQuotes = append(aggQuotes, aggQuote)
            }
        }
        
        // Create indexes from created indexes
        for key, value := range tiifx.indices {
            aggQuote := NewQuote(key, 0)
            for _, quote := range aggQuotes {
                for _, symbol := range value {
                    if quote.Symbol == symbol {
                        if len(quote.Epoch) > 0 {
                            if len(aggQuote.Epoch) == 0 && len(quote.Epoch) > 0 {
                                aggQuote.Epoch = quote.Epoch
                                aggQuote.Open = quote.Open
                                aggQuote.High = quote.High
                                aggQuote.Low = quote.Low
                                aggQuote.Close = quote.Close
                                aggQuote.Volume = quote.Volume
                            } else if len(aggQuote.Epoch) == len(quote.Epoch) && aggQuote.Epoch[0] == quote.Epoch[0] && aggQuote.Epoch[len(aggQuote.Epoch)-1] == quote.Epoch[len(quote.Epoch)-1] {
                                // aggQuote (Index) and quote (new symbol to be added) matches in row length and start/end points
                                numrows := len(aggQuote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    // Calculate the market capitalization
                                    quoteCap := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), big.NewFloat(quote.Volume[bar]))
                                    aggQuoteCap := new(big.Float).Mul(big.NewFloat(aggQuote.Close[bar]), big.NewFloat(aggQuote.Volume[bar]))
                                    totalCap := new(big.Float).Add(quoteCap, aggQuoteCap)
                                    // Calculate the weighted averages
                                    quoteWeight := new(big.Float).Quo(quoteCap, totalCap)
                                    aggQuoteWeight := new(big.Float).Quo(aggQuoteCap, totalCap)
                                    
                                    weightedOpen := new(big.Float).Mul(big.NewFloat(quote.Open[bar]), quoteWeight)
                                    weightedOpen = weightedOpen.Add(weightedOpen, new(big.Float).Mul(big.NewFloat(aggQuote.Open[bar]), aggQuoteWeight))
                                    
                                    weightedHigh := new(big.Float).Mul(big.NewFloat(quote.High[bar]), quoteWeight)
                                    weightedHigh = weightedHigh.Add(weightedHigh, new(big.Float).Mul(big.NewFloat(aggQuote.High[bar]), aggQuoteWeight))
                                    
                                    weightedLow := new(big.Float).Mul(big.NewFloat(quote.Low[bar]), quoteWeight)
                                    weightedLow = weightedLow.Add(weightedLow, new(big.Float).Mul(big.NewFloat(aggQuote.Low[bar]), aggQuoteWeight))
                                    
                                    weightedClose := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), quoteWeight)
                                    weightedClose = weightedClose.Add(weightedClose, new(big.Float).Mul(big.NewFloat(aggQuote.Close[bar]), aggQuoteWeight))
                                    
                                    aggQuote.Open[bar], _ = weightedOpen.Float64()
                                    aggQuote.High[bar], _ = weightedHigh.Float64()
                                    aggQuote.Low[bar], _ = weightedLow.Float64()
                                    aggQuote.Close[bar], _ = weightedClose.Float64()
                                    aggQuote.Volume[bar], _ = totalCap.Quo(totalCap, weightedClose).Float64()
                                }
                            } else if len(aggQuote.Epoch) > 0 && len(quote.Epoch) > 0 {
                                // aggQuote (Index) and quote (new symbol to be added) does not match in row length or start/end points
                                numrows := len(quote.Epoch)
                                for bar := 0; bar < numrows; bar++ {
                                    matchedEpochs := false
                                    matchedBar    := bar
                                    // First Test
                                    if len(aggQuote.Epoch) > bar {
                                        if quote.Epoch[bar] == aggQuote.Epoch[bar] {
                                            // Shallow Iteration on quote matches with aggQuote
                                            matchedEpochs = true
                                            matchedBar = bar
                                        }
                                    }
                                    // Second Test
                                    if !matchedEpochs {
                                        // Nested Iteration on aggQuote to match quote with aggQuote
                                        numrows2 := len(aggQuote.Epoch)
                                        for bar2 := 0; bar2 < numrows2; bar2++ {
                                            if quote.Epoch[bar] == aggQuote.Epoch[bar2] {
                                                matchedEpochs = true
                                                matchedBar = bar2
                                                break
                                            }
                                        }
                                    }
                                    if !matchedEpochs {
                                        // If no Epochs were matched, it means quote contains Epoch that aggQuote does not have
                                        aggQuote.Epoch = append(aggQuote.Epoch, quote.Epoch[bar])
                                        aggQuote.Open = append(aggQuote.Open, quote.Open[bar])
                                        aggQuote.High = append(aggQuote.High, quote.High[bar])
                                        aggQuote.Low = append(aggQuote.Low, quote.Low[bar])
                                        aggQuote.Close = append(aggQuote.Close, quote.Close[bar])
                                        aggQuote.Volume = append(aggQuote.Volume, quote.Volume[bar])
                                    } else {
                                        // Calculate the market capitalization
                                        quoteCap := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), big.NewFloat(quote.Volume[bar]))
                                        aggQuoteCap := new(big.Float).Mul(big.NewFloat(aggQuote.Close[matchedBar]), big.NewFloat(aggQuote.Volume[matchedBar]))
                                        totalCap := new(big.Float).Add(quoteCap, aggQuoteCap)
                                        // Calculate the weighted averages
                                        quoteWeight := new(big.Float).Quo(quoteCap, totalCap)
                                        aggQuoteWeight := new(big.Float).Quo(aggQuoteCap, totalCap)
                                        
                                        weightedOpen := new(big.Float).Mul(big.NewFloat(quote.Open[bar]), quoteWeight)
                                        weightedOpen = weightedOpen.Add(weightedOpen, new(big.Float).Mul(big.NewFloat(aggQuote.Open[matchedBar]), aggQuoteWeight))
                                        
                                        weightedHigh := new(big.Float).Mul(big.NewFloat(quote.High[bar]), quoteWeight)
                                        weightedHigh = weightedHigh.Add(weightedHigh, new(big.Float).Mul(big.NewFloat(aggQuote.High[matchedBar]), aggQuoteWeight))
                                        
                                        weightedLow := new(big.Float).Mul(big.NewFloat(quote.Low[bar]), quoteWeight)
                                        weightedLow = weightedLow.Add(weightedLow, new(big.Float).Mul(big.NewFloat(aggQuote.Low[matchedBar]), aggQuoteWeight))
                                        
                                        weightedClose := new(big.Float).Mul(big.NewFloat(quote.Close[bar]), quoteWeight)
                                        weightedClose = weightedClose.Add(weightedClose, new(big.Float).Mul(big.NewFloat(aggQuote.Close[matchedBar]), aggQuoteWeight))
                                        
                                        aggQuote.Open[matchedBar], _ = weightedOpen.Float64()
                                        aggQuote.High[matchedBar], _ = weightedHigh.Float64()
                                        aggQuote.Low[matchedBar], _ = weightedLow.Float64()
                                        aggQuote.Close[matchedBar], _ = weightedClose.Float64()
                                        aggQuote.Volume[matchedBar], _ = totalCap.Quo(totalCap, weightedClose).Float64()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if len(aggQuote.Epoch) > 0 {
                aggQuotes = append(aggQuotes, aggQuote)
            }
        }
        for _, quote := range aggQuotes {
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            cs.AddColumn("Volume", quote.Volume)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLCV")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
            
            log.Debug("Forex: %v index row(s) to %s/%s/OHLCV from %v to %v by Aggregation", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
        }
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiifx.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 3, 0, time.UTC)
            // Check if timeEnd is Closing, will return Opening if so
            openTime := alignTimeToTradingHours(timeEnd, calendar)
            if openTime != timeEnd {
                // Set to wait till Opening
                waitTill = openTime
            }
            log.Info("Forex: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*30)
		}
	}
}

func main() {
}
