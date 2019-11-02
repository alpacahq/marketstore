package main

import (
	"encoding/json"
	"fmt"
    // "errors"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
    // "strconv"
    //"strings"
	"time"
    "math/rand"
    //"math/big"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
	"gopkg.in/yaml.v2"
	"github.com/alpacahq/marketstore/quanatee/plugins/quanatee_stock_prices/calendar"    
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
	HLC       []float64   `json:"HLC"`
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
		HLC:    make([]float64, bars),
		Volume: make([]float64, bars),
	}
}

func GetPolygonPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, calendar *cal.Calendar, token string) (Quote, error) {
    
	resampleFreq := "5"
	switch period.String {
	case "1Min":
		resampleFreq = "1"
	case "5Min":
		resampleFreq = "5"
	case "15Min":
		resampleFreq = "15"
	case "30Min":
		resampleFreq = "30"
	}
    
	type priceData struct {
        Ticker         string  `json:"T"`
		Volume         float64 `json:"v"`
		Open           float64 `json:"o"`
		High           float64 `json:"h"`
		Low            float64 `json:"l"`
		Close          float64 `json:"c"`
		Timestamp      int64   `json:"t"`
		Items          int64   `json:"n"`
	}
    
	type polygonData struct {
        Symbol          string        `json:"ticker"`
		Status          string        `json:"status"`
		Adjusted        bool          `json:"adjusted"`
		queryCount      int64         `json:"queryCount"`
		resultsCount    int64         `json:"resultsCount"`
        PriceData       []priceData   `json:"results"`
	}
    
    var forexData polygonData
    // https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2019-01-01/2019-02-01?unadjusted=true&apiKey=
    apiUrl := fmt.Sprintf(
                        "https://api.polygon.io/v2/aggs/ticker/%s/range/%s/minute/%s/%s?unadjusted=false&apiKey=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.AddDate(0, 0, -1).Format("2006-01-02")),
                        url.QueryEscape(to.Format("2006-01-02")),
                        token)
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", apiUrl, nil)
	//req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := client.Do(req)
    
    // Try again if fail
	if err != nil {
        time.Sleep(1 * time.Second)    
        resp, err = client.Do(req)
    }
    
	if err != nil {
		log.Warn("Forex: Polygon symbol '%s' error: %s \n %s", symbol, err, apiUrl)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Warn("Forex: Polygon symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData.PriceData) < 1 {
        if ( calendar.IsWorkday(from.UTC()) && 
           (( int(from.UTC().Weekday()) == 1 && from.UTC().Hour() >= 7 ) || 
            ( int(from.UTC().Weekday()) >= 2 && int(from.UTC().Weekday()) <= 4 ) || 
            ( int(from.UTC().Weekday()) == 5 && from.UTC().Hour() < 21 )  || 
            ( int(from.UTC().Weekday()) == 5 && from.UTC().Hour() == 21 && from.UTC().Minute() == 0 )) ) {
            log.Warn("Forex: Polygon symbol '%s' No data returned from %v-%v, \n %s", symbol, from, to, apiUrl)
        }
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(forexData.PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt := time.Unix(0, forexData.PriceData[bar].Timestamp * int64(1000000)) //Timestamp is in milliseconds    
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if ( calendar.IsWorkday(dt.UTC()) && 
           (( int(dt.UTC().Weekday()) == 1 && dt.UTC().Hour() >= 7 ) || 
            ( int(dt.UTC().Weekday()) >= 2 && int(dt.UTC().Weekday()) <= 4 ) || 
            ( int(dt.UTC().Weekday()) == 5 && dt.UTC().Hour() < 21 )  || 
            ( int(dt.UTC().Weekday()) == 5 && dt.UTC().Hour() == 21 && dt.UTC().Minute() == 0 )) ) {
            if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
                if startOfSlice == -1 {
                    startOfSlice = bar
                }
                endOfSlice = bar
                quote.Epoch[bar] = dt.UTC().Unix()
                quote.Open[bar] = forexData.PriceData[bar].Open
                quote.High[bar] = forexData.PriceData[bar].High
                quote.Low[bar] = forexData.PriceData[bar].Low
                quote.Close[bar] = forexData.PriceData[bar].Close
                quote.HLC[bar] = (forexData.PriceData[bar].High + forexData.PriceData[bar].Low + forexData.PriceData[bar].Close)/3
                quote.Volume[bar] = forexData.PriceData[bar].Volume
            }
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
        quote.HLC = quote.HLC[startOfSlice:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    /*
    // DEPRECATED BUT KEPT FOR REFERENCE
    // Reverse the order of slice in Intrinio because data is returned in descending (latest to earliest) whereas Tiingo does it from ascending (earliest to latest)
    for i, j := 0, len(quote.Epoch)-1; i < j; i, j = i+1, j-1 {
        quote.Epoch[i], quote.Epoch[j] = quote.Epoch[j], quote.Epoch[i]
        quote.Open[i], quote.Open[j] = quote.Open[j], quote.Open[i]
        quote.High[i], quote.High[j] = quote.High[j], quote.High[i]
        quote.Low[i], quote.Low[j] = quote.Low[j], quote.Low[i]
        quote.Close[i], quote.Close[j] = quote.Close[j], quote.Close[i]
        quote.HLC[i], quote.HLC[j] = quote.HLC[j], quote.HLC[i]
        quote.Volume[i], quote.Volume[j] = quote.Volume[j], quote.Volume[i]
    }
    */

	return quote, nil
}

func GetTiingoPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, calendar *cal.Calendar, token string) (Quote, error) {

	resampleFreq := "1min"
	switch period.String {
	case "1Min":
		resampleFreq = "1min"
	case "5Min":
		resampleFreq = "5min"
	case "15Min":
		resampleFreq = "15min"
	case "30Min":
		resampleFreq = "30min"
	}

	type priceData struct {
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		Ticker         string  `json:"ticker"`
		Open           float64 `json:"open"`
		Low            float64 `json:"low"`
		High           float64 `json:"high"`
		Close          float64 `json:"close"`
	}
    
	type dailyData struct {
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		Open           float64 `json:"open"`
		Low            float64 `json:"low"`
		High           float64 `json:"high"`
		Close          float64 `json:"close"`
		Volume         float64 `json:"volume"`
		AdjOpen        float64 `json:"adjOpen"`
		AdjLow         float64 `json:"adjLow"`
		AdjHigh        float64 `json:"adjHigh"`
		AdjClose       float64 `json:"adjClose"`
		AdjVolume      float64 `json:"adjVolume"`
	}
    
	var iexData  []priceData
	var iexDaily []dailyData
    
    apiUrl := fmt.Sprintf(
                        "https://api.tiingo.com/iex/%s/prices?resampleFreq=%s&afterHours=false&forceFill=false&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
                        
    // For getting volume data
    apiUrl2 := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s",
                        symbol,
                        url.QueryEscape(from.AddDate(0, 0, -7).Format("2006-1-2")))
    
    if !realTime {
        apiUrl = apiUrl + "&endDate=" + url.QueryEscape(to.Format("2006-1-2"))
        apiUrl2 = apiUrl2 + "&endDate=" + url.QueryEscape(to.Format("2006-1-2"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", apiUrl, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)
    
	req2, _ := http.NewRequest("GET", apiUrl2, nil)
	req2.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp2, err2 := client.Do(req2)
    
    // Try again if fail
	if err != nil || err2 != nil {
        time.Sleep(250 * time.Millisecond)
        resp, err = client.Do(req)
        resp2, err2 = client.Do(req2)
    }
    
	if err != nil || err2 != nil {
		log.Warn("Stock: Tiingo symbol '%s' error: %s, error2: %s \n %s \n %s", symbol, err, err2, apiUrl, apiUrl2)
        if err != nil {
            return NewQuote(symbol, 0), err
        } else {
            return NewQuote(symbol, 0), err2
        }
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &iexData)
    
	contents2, _ := ioutil.ReadAll(resp2.Body)
	err2 = json.Unmarshal(contents2, &iexDaily)
    
	if err != nil || err2 != nil {
		log.Warn("Stock: Tiingo symbol '%s' error: %v, error2: %v \n contents: %s", symbol, err, err2, contents)
        if err != nil {
            return NewQuote(symbol, 0), err
        } else {
            return NewQuote(symbol, 0), err2
        }
	}
    
    if len(iexData) < 1 {
        // NYSE opening time from 13:30 to 21:00
        if ( calendar.IsWorkday(from) && ( int(from.Weekday()) >= 1 && int(from.Weekday()) <= 5 && ( ( from.Hour() == 13 && from.Minute() >= 30 ) || from.Hour() >= 14 ) && ( from.Hour() < 20 ) ) ) {
            log.Warn("Stock: Tiingo symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, apiUrl)
        }
 		return NewQuote(symbol, 0), err
	}
    
    if len(iexDaily) < 1 {
        log.Warn("Stock: Tiingo symbol '%s' No daily data returned url %s", symbol, apiUrl2)
 		return NewQuote(symbol, 0), err2
	}
    
	numrows := len(iexData)
	numdays := len(iexDaily)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, iexData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = iexData[bar].Open
            quote.High[bar] = iexData[bar].High
            quote.Low[bar] = iexData[bar].Low
            quote.Close[bar] = iexData[bar].Close
            quote.HLC[bar] = (quote.High[bar] + quote.Low[bar] + quote.Close[bar])/3
            // Find the previous valid workday
            previousWorkday := false
            days := 1
            for previousWorkday == false {
                if calendar.IsWorkday(dt.AddDate(0, 0, -days)) {
                    previousWorkday = true
                    break
                } else {
                    days += 1
                }
            }
            // Add volume from previous daily price data
            for bar2 := 0; bar2 < numdays; bar2++ {
                dt2, _ := time.Parse(time.RFC3339, iexDaily[bar2].Date)
                if dt.AddDate(0, 0, -days) == dt2 {
                    quote.Volume[bar] = iexDaily[bar2].AdjVolume
                } else {
                    quote.Volume[bar] = 1.0
                }
            }
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
        quote.HLC = quote.HLC[startOfSlice:endOfSlice+1]
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
}

type FetcherConfig struct {
	Symbols        []string `yaml:"symbols"`
    ApiKey         string    `yaml:"api_key"`
    ApiKey2        string    `yaml:"api_key2"`
	QueryStart     string    `yaml:"query_start"`
	BaseTimeframe  string    `yaml:"base_timeframe"`
}

// IEXFetcher is the main worker for TiingoIEX
type IEXFetcher struct {
	config         map[string]interface{}
	symbols        []string
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
    
    // NYSE Opening = 1330 UTC is the first data we will consume in a session
    // NYSE Closing = 2100 UTC is the last data we will consume in a session
    // We do not account for disruptions in Marketstore
    // Aligning time series datas is done in Quanatee functions

    if !calendar.IsWorkday(timeCheck) || ( !calendar.IsWorkday(timeCheck.AddDate(0, 0, 1)) && timeCheck.Hour() > 21 ) {
        // Current date is not a Work Day, or next day is not a Work Day and current Work Day has ended
        // Find the next Work Day and set to Opening
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
        timeCheck = time.Date(timeCheck.Year(), timeCheck.Month(), timeCheck.Day(), 13, 30, 0, 0, time.UTC)
    }
    
    return timeCheck
}
   
// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
	return &IEXFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
        apiKey2:        config.ApiKey2,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiieq *IEXFetcher) Run() {

	realTime := false    
	timeStart := time.Time{}
	lastTimestamp := time.Time{}
    
    // Get last timestamp collected
	for _, symbol := range tiieq.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiieq.baseTimeframe.String + "/Price")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("Stock: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
    calendar := cal.NewCalendar()

    // Add US holidays
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
    )
    
	// Set start time if not given.
	if !tiieq.queryStart.IsZero() {
		timeStart = tiieq.queryStart.UTC()
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
            timeEnd = timeStart.Add(tiieq.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.AddDate(0, 0, 7)
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
        symbols := tiieq.symbols
        rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
        // Data for symbols are retrieved in random order for fairness
        // Data for symbols are written immediately for asynchronous-like processing
        for _, symbol := range symbols {
            tiingoQuote, _ := GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiieq.baseTimeframe, calendar, tiieq.apiKey)
            polygonQuote, _ := GetPolygonPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiieq.baseTimeframe, calendar, tiieq.apiKey2)
            quote := NewQuote(symbol, 0)
            if len(polygonQuote.Epoch) == len(tiingoQuote.Epoch) {
                quote = polygonQuote
                numrows := len(polygonQuote.Epoch)
                for bar := 0; bar < numrows; bar++ {
                    quote.Open[bar] = (quote.Open[bar] + tiingoQuote.Open[bar]) / 2
                    quote.High[bar] = (quote.High[bar] + tiingoQuote.High[bar]) / 2
                    quote.Low[bar] = (quote.Low[bar] + tiingoQuote.Low[bar]) / 2
                    quote.Close[bar] = (quote.Close[bar] + tiingoQuote.Close[bar]) / 2
                    quote.HLC[bar] = (quote.HLC[bar] + tiingoQuote.HLC[bar]) / 2
                    quote.Volume[bar] = (quote.Volume[bar] + tiingoQuote.Volume[bar])
                }
                dataProvider = "Even Aggregation"
            } else if len(polygonQuote.Epoch) > 0 && len(tiingoQuote.Epoch) > 0 {
                quote2 := NewQuote(symbol, 0)
                if len(polygonQuote.Epoch) > len(tiingoQuote.Epoch) {
                    quote = polygonQuote
                    quote2 = tiingoQuote
                } else {
                    quote = tiingoQuote
                    quote2 = polygonQuote
                }
                for bar := 0; bar < len(quote.Epoch); bar++ {
                    // Test if they both have the same Epochs in the same bar (position)
                    if len(quote2.Epoch) > bar { // Check if quote2 has enough length first
                        if quote.Epoch[bar] == quote2.Epoch[bar] {
                            quote.Open[bar] = (quote.Open[bar] + quote2.Open[bar]) / 2
                            quote.High[bar] = (quote.High[bar] + quote2.High[bar]) / 2
                            quote.Low[bar] = (quote.Low[bar] + quote2.Low[bar]) / 2
                            quote.Close[bar] = (quote.Close[bar] + quote2.Close[bar]) / 2
                            quote.HLC[bar] = (quote.HLC[bar] + quote2.HLC[bar]) / 2
                            quote.Volume[bar] = (quote.Volume[bar] + quote2.Volume[bar])
                            continue
                        }
                    }
                    // Test if they both have the same Epochs, but in different bars
                    for bar2 := 0; bar2 < len(quote2.Epoch); bar2++ {
                        if quote.Epoch[bar] == quote2.Epoch[bar2] {
                            quote.Open[bar] = (quote.Open[bar] + quote2.Open[bar2]) / 2
                            quote.High[bar] = (quote.High[bar] + quote2.High[bar2]) / 2
                            quote.Low[bar] = (quote.Low[bar] + quote2.Low[bar2]) / 2
                            quote.Close[bar] = (quote.Close[bar] + quote2.Close[bar2]) / 2
                            quote.HLC[bar] = (quote.HLC[bar] + quote2.HLC[bar2]) / 2
                            quote.Volume[bar] = (quote.Volume[bar] + quote2.Volume[bar2])
                            break
                        }
                    }
                }
                dataProvider = "Odd Aggregation"
            } else if len(polygonQuote.Epoch) > 0 && polygonQuote.Epoch[0] > 0 && polygonQuote.Epoch[len(polygonQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = polygonQuote
                dataProvider = "Polygon"
            } else if len(tiingoQuote.Epoch) > 0 && tiingoQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0 {
                // Only one quote is valid
                quote = tiingoQuote
                dataProvider = "Tiingo"
            } else {
                dataProvider = "None"
                continue
            }
            
            if len(quote.Epoch) < 1 {
                // Check if there is data to add
                continue
            } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                // Check if realTime is adding the most recent data
                log.Warn("Stock: Previous row dated %v is still the latest in %s/%s/Price", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiieq.baseTimeframe.String)
                continue
            }
            // write to csm
            cs := io.NewColumnSeries()
            cs.AddColumn("Epoch", quote.Epoch)
            cs.AddColumn("Open", quote.Open)
            cs.AddColumn("High", quote.High)
            cs.AddColumn("Low", quote.Low)
            cs.AddColumn("Close", quote.Close)
            cs.AddColumn("HLC", quote.HLC)
            cs.AddColumn("Volume", quote.Volume)
            csm := io.NewColumnSeriesMap()
            tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiieq.baseTimeframe.String + "/Price")
            csm.AddColumnSeries(*tbk, cs)
            executor.WriteCSM(csm, false)
            
            log.Info("Stock: %v row(s) to %s/%s/Price from %v to %v by %s ", len(quote.Epoch), quote.Symbol, tiieq.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
            quotes = append(quotes, quote)
        }
        
        // Save the latest timestamp written
        if len(quotes) > 0 {
            if len(quotes[0].Epoch) > 0{
                lastTimestamp = time.Unix(quotes[0].Epoch[len(quotes[0].Epoch)-1], 0)
            }
        }

        if realTime {
            for {
                if time.Now().UTC().Unix() > timeEnd.Add(tiieq.baseTimeframe.Duration).UTC().Unix() && alignTimeToTradingHours(timeEnd, calendar) == timeEnd {
                    break
                } else {
                    time.Sleep(time.Second*1)
                }
            }
        } else {
			time.Sleep(time.Second*60)
        }

	}
}

func main() {
}
