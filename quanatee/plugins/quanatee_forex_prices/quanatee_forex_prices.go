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
    "reflect"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
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
}

// Quotes - an array of historical price data
type Quotes []Quote

// ClientTimeout - connect/read timeout for client requests
const ClientTimeout = 10 * time.Second

// NewQuote - new empty Quote struct
func NewQuote(symbol string, bars int) Quote {
	return Quote{
		Symbol: symbol,
		Epoch:  make([]int64,   bars),
		Open:   make([]float64, bars),
		High:   make([]float64, bars),
		Low:    make([]float64, bars),
		Close:  make([]float64, bars),
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

    api_url := fmt.Sprintf(
                        "https://api-v2.intrinio.com/forex/prices/%s/%s?api_key=%s&start_date=%s&start_time=%s",
                        symbol,
                        resampleFreq,
                        token,
                        url.QueryEscape(from.Add(-period.Duration).Format("2006-01-02")),
                        url.QueryEscape(from.Add(-period.Duration).Format("15:04:05")))
    
    if !realTime {
        api_url = api_url + "&end_date=" + url.QueryEscape(to.Format("2006-01-02")) + "&end_time=" + url.QueryEscape(to.Format("15:04:05"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", api_url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := client.Do(req)
    
	if err != nil {
		log.Info("Forex: Intrinio symbol '%s' error: %s \n %s", symbol, err, api_url)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Info(": Intrinio symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData.PriceData) < 1 {
        if ( ( !realTime && calendar.IsWorkday(from) && calendar.IsWorkday(to) ) || ( realTime && calendar.IsWorkday(from) && ( ( int(from.Weekday()) == 1 && from.Hour() >= 7 ) || ( int(from.Weekday()) == 5 && from.Hour() < 21 ) ) ) ) {
            log.Warn("Forex: Intrinio symbol '%s' No data returned from %v-%v, \n %s", symbol, from, to, api_url)
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
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
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
    
    api_url := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/fx/%s/prices?resampleFreq=%s&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    // Pad to with an extra day if backfilling to ensure that start_date and end_date is different
    if !realTime && to.AddDate(0, 0, 1).After(time.Now().UTC()) {
        api_url = api_url + "&endDate=" + url.QueryEscape(to.AddDate(0, 0, 1).Format("2006-1-2"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", api_url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)
    
	if err != nil {
		log.Info("Forex: Tiingo symbol '%s' error: %s \n %s", symbol, err, api_url)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &forexData)
	if err != nil {
		log.Info("Forex: Tiingo symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(forexData) < 1 {
        /*
        if ( ( !realTime && calendar.IsWorkday(from) && calendar.IsWorkday(to) ) || ( realTime && calendar.IsWorkday(from) && ( ( int(from.Weekday()) == 1 && from.Hour() >= 7 ) || ( int(from.Weekday()) == 5 && from.Hour() < 21 ) ) ) ) {
            log.Warn("Forex: Tiingo symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, api_url)
        }
        */
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
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice+1]
        quote.Open = quote.Open[startOfSlice:endOfSlice+1]
        quote.High = quote.High[startOfSlice:endOfSlice+1]
        quote.Low = quote.Low[startOfSlice:endOfSlice+1]
        quote.Close = quote.Close[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
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

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	Symbols        []string `json:"symbols"`
	BTCX           []string  `json:"BTCZ"`
	USDX           []string  `json:"USDZ"`
	EURX           []string  `json:"EURZ"`
	JPYZ           []string  `json:"JPYZ"`
    ApiKey         string   `json:"api_key"`
    ApiKey2        string   `json:"api_key2"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
}

// ForexFetcher is the main worker for TiingoForex
type ForexFetcher struct {
	config         map[string]interface{}
	symbols        []string
	aggSymbols    map[string][]string
    apiKey         string
    apiKey2        string
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
            }
            days += days
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

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}
    
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
    aggSymbols := map[string][]string{
        "USDX": config.USDX,
        "EURX": config.EURX,
        "JPYX": config.JPYX,
    }
    
	return &ForexFetcher{
		config:         conf,
		symbols:        symbols,
		aggSymbols:     aggSymbols,
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
        tbk := io.NewTimeBucketKey(symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
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
        
        if !firstLoop {
            
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
                // If both Quotes have valid datas, combine them
                // If not, serve only the quote with valid datas
                // Both Quotes would have the same length since we only add datas according to the requested period range
                if len(tiingoQuote.Epoch) < 1 && len(intrinioQuote.Epoch) < 1 {
                    // Both quotes are invalid
                    continue
                } else if len(tiingoQuote.Epoch) == len(intrinioQuote.Epoch) && tiingoQuote.Epoch[0] > 0 && intrinioQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0 && intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] > 0 {
                    // Both quotes are valid
                    if tiingoQuote.Epoch[0] != intrinioQuote.Epoch[0] || tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] != intrinioQuote.Epoch[len(intrinioQuote.Epoch)-1] {
                        // First and last epochs do not match
                        // This could be either datas returned are in different orders; or
                        // Datas returned have missing data rows (likely from Tiingo); or
                        // Improper slicing of periods
                        log.Info("Forex: %s Tiingo and Intrinio do not match in Epochs!", symbol)
                        quote = intrinioQuote
                        dataProvider = "Intrinio"
                    } else {
                        // First and last epochs match, we assume that the rows are lined up
                        numrows := len(intrinioQuote.Epoch)
                        quote = NewQuote(symbol, numrows)
                        for bar := 0; bar < numrows; bar++ {
                            if tiingoQuote.Epoch[bar] != intrinioQuote.Epoch[bar] {
                                // If the rows are not lined up, we fallback to Intrinio only
                                log.Info("Forex: %s mismatched Epochs during aggregation %v, %v", symbol, tiingoQuote.Epoch[bar], intrinioQuote.Epoch[bar])
                                quote.Epoch[bar] = intrinioQuote.Epoch[bar]
                                quote.Open[bar] = intrinioQuote.Open[bar]
                                quote.High[bar] = intrinioQuote.High[bar]
                                quote.Low[bar] = intrinioQuote.Low[bar]
                                quote.Close[bar] = intrinioQuote.Close[bar]
                            } else {
                                quote.Epoch[bar] = tiingoQuote.Epoch[bar]
                                quote.Open[bar] = (tiingoQuote.Open[bar] + intrinioQuote.Open[bar]) / 2
                                quote.High[bar] = (tiingoQuote.High[bar] + intrinioQuote.High[bar]) / 2
                                quote.Low[bar] = (tiingoQuote.Low[bar] + intrinioQuote.Low[bar]) / 2
                                quote.Close[bar] = (tiingoQuote.Close[bar] + intrinioQuote.Close[bar]) / 2
                            }
                        }
                        dataProvider = "Aggregation"
                    }
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
                    log.Info("IEX: Previous row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiifx.baseTimeframe.String)
                    continue
                }
                // write to csm
                cs := io.NewColumnSeries()
                cs.AddColumn("Epoch", quote.Epoch)
                cs.AddColumn("Open", quote.Open)
                cs.AddColumn("High", quote.High)
                cs.AddColumn("Low", quote.Low)
                cs.AddColumn("Close", quote.Close)
                csm := io.NewColumnSeriesMap()
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                // Save the latest timestamp written
                lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0)
                log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v by %s", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
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
                    }
                    // write to csm
                    cs := io.NewColumnSeries()
                    cs.AddColumn("Epoch", revQuote.Epoch)
                    cs.AddColumn("Open", revQuote.Open)
                    cs.AddColumn("High", revQuote.High)
                    cs.AddColumn("Low", revQuote.Low)
                    cs.AddColumn("Close", revQuote.Close)
                    csm := io.NewColumnSeriesMap()
                    tbk := io.NewTimeBucketKey(revQuote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    // log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v", len(revQuote.Epoch), revQuote.Symbol, tiifx.baseTimeframe.String, time.Unix(revQuote.Epoch[0], 0).UTC(), time.Unix(revQuote.Epoch[len(revQuote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, revQuote)
                }
            }
            
            aggQuotes := Quotes{}
            // Convert keys (int) into strings
            keys := reflect.ValueOf(tiifx.symbols).MapKeys()
            aggSymbols := make([]string, len(keys))
            for i := 0; i < len(keys); i++ {
                aggSymbols[i] = keys[i].String()
            }
            for key, symbols := range tiifx.symbols {
                aggQuote := NewQuote(aggSymbols[key], 0)
                for _, quote := range quotes {
                    for _, symbol := range symbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(aggQuote.Epoch) == 0 {
                                    aggQuote.Epoch = quote.Epoch
                                    aggQuote.Open = quote.Open
                                    aggQuote.High = quote.High
                                    aggQuote.Low = quote.Low
                                    aggQuote.Close = quote.Close
                                } else if len(aggQuote.Epoch) == len(quote.Epoch) {
                                    numrows := len(aggQuote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        aggQuote.Open[bar] = (quote.Open[bar] + aggQuote.Open[bar]) / 2
                                        aggQuote.High[bar] = (quote.High[bar] + aggQuote.High[bar]) / 2
                                        aggQuote.Low[bar] = (quote.Low[bar] + aggQuote.Low[bar]) / 2
                                        aggQuote.Close[bar] = (quote.Close[bar] + aggQuote.Close[bar]) / 2
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
                csm := io.NewColumnSeriesMap()
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiifx.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("Forex: %v row(s) to %s/%s/OHLC from %v to %v by %s", len(quote.Epoch), quote.Symbol, tiifx.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
            }
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
			time.Sleep(time.Second*4)
		}
	}
}

func main() {
}
