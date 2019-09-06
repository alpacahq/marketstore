package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    "math/rand"
    "math/big"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
	"gopkg.in/yaml.v2"
	"github.com/alpacahq/marketstore/quanatee/plugins/quanatee_iex_prices/calendar"    
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
                        "https://api.tiingo.com/iex/%s/prices?resampleFreq=%s&afterHours=true&forceFill=true&startDate=%s",
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
        time.Sleep(100 * time.Millisecond)
        resp, err = client.Do(req)
        resp2, err2 = client.Do(req2)
    }
    
	if err != nil || err2 != nil {
		log.Info("IEX: symbol '%s' error: %s, error2: %s \n %s \n %s", symbol, err, err2, apiUrl, apiUrl2)
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
		log.Info("IEX: symbol '%s' error: %v, error2: %v \n contents: %s", symbol, err, err2, contents)
        if err != nil {
            return NewQuote(symbol, 0), err
        } else {
            return NewQuote(symbol, 0), err2
        }
	}
    
    if len(iexData) < 1 {
        if ( ( !realTime && calendar.IsWorkday(from) && calendar.IsWorkday(to) ) || ( realTime && calendar.IsWorkday(from) && ( ( from.Hour() >= 12 ) && ( ( from.Hour() < 22 ) || ( from.Hour() == 22 && from.Minute() <= 30 ) ) ) ) ) {
            log.Warn("IEX: symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, apiUrl)
        }
 		return NewQuote(symbol, 0), err
	}
    
    if len(iexDaily) < 1 {
        log.Warn("IEX: symbol '%s' No daily data returned url %s", symbol, apiUrl2)
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
            // Find the previous valid workday
            previousWorkday := false
            days := 1
            for previousWorkday == false {
                if calendar.IsWorkday(dt.AddDate(0, 0, -days)) {
                    previousWorkday = true
                } else {
                    days += days
                }
            }
            // Add volume from previous daily price data
            for bar2 := 0; bar2 < numdays; bar2++ {
                dt2, _ := time.Parse(time.RFC3339, iexDaily[bar2].Date)
                if dt.AddDate(0, 0, -days) == dt2 {
                    quote.Volume[bar] = iexDaily[bar2].AdjVolume
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
        quote.Volume = quote.Volume[startOfSlice:endOfSlice+1]
    } else {
        quote = NewQuote(symbol, 0)
    }
    
	return quote, nil
}

type FetcherConfig struct {
	Symbols        []string `yaml:"symbols"`
    Indices        map[string][]string `yaml:"indices"`
    ApiKey         string    `yaml:"api_key"`
	QueryStart     string    `yaml:"query_start"`
	BaseTimeframe  string    `yaml:"base_timeframe"`
}

// IEXFetcher is the main worker for TiingoIEX
type IEXFetcher struct {
	config         map[string]interface{}
	symbols        []string
	indices        map[string][]string
    apiKey         string
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
    
    // NYSE Opening = 1200 UTC is the first data we will consume in a session
    // NYSE Closing = 2130 UTC is the last data we will consume in a session
    // We do not account for disruptions in Marketstore
    // Aligning time series datas is done in Quanatee functions

    if !calendar.IsWorkday(timeCheck) || ( !calendar.IsWorkday(timeCheck.AddDate(0, 0, 1)) && ( (timeCheck.Hour() == 22 && timeCheck.Minute() >= 30) || ( timeCheck.Hour() > 23 ) ) ) {
        // Current date is not a Work Day, or next day is not a Work Day and current Work Day has ended
        // Find the next Work Day and set to Opening
        nextWorkday := false
        days := 1
        for nextWorkday == false {
            if calendar.IsWorkday(timeCheck.AddDate(0, 0, days)) {
                nextWorkday = true
            } else {
                days += days
            }
        }
        timeCheck = timeCheck.AddDate(0, 0, days)
        timeCheck = time.Date(timeCheck.Year(), timeCheck.Month(), timeCheck.Day(), 13, 0, 0, 0, time.UTC)
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
	}
    
	if len(config.Indices) > 0 {
		indices = config.Indices
	}
    
	return &IEXFetcher{
		config:         conf,
		symbols:        symbols,
		indices:        indices,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiiex *IEXFetcher) Run() {

	realTime := false    
	timeStart := time.Time{}
	lastTimestamp := time.Time{}
    
    // Get last timestamp collected
	for _, symbol := range tiiex.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiiex.baseTimeframe.String + "/OHLCV")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("IEX: lastTimestamp for %s = %v", symbol, lastTimestamp)
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
	if !tiiex.queryStart.IsZero() {
		timeStart = tiiex.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC()
	}
    timeStart = alignTimeToTradingHours(timeStart, calendar)
    
	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if firstLoop {
            firstLoop = false
        } else {
            timeStart = timeEnd
        }
        if realTime {
            // Add timeEnd by a tick
            timeEnd = timeStart.Add(tiiex.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.AddDate(0, 0, 1)
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
            symbols := tiiex.symbols
            rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
            // Data for symbols are retrieved in random order for fairness
            // Data for symbols are written immediately for asynchronous-like processing
            for _, symbol := range symbols {
                time.Sleep(150 * time.Millisecond)
                time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
                quote, err := GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiiex.baseTimeframe, calendar, tiiex.apiKey)
                if err == nil {
                    if len(quote.Epoch) < 1 {
                        // Check if there is data to add
                        continue
                    } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                        // Check if realTime is adding the most recent data
                        log.Info("IEX: Previous row dated %v is still the latest in %s/%s/OHLCV", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiiex.baseTimeframe.String)
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
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLCV")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    // Save the latest timestamp written
                    lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0)
                    log.Info("IEX: %v row(s) to %s/%s/OHLCV from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, quote)
                } else {
                    log.Info("IEX: error downloading " + symbol)
                }
            }
            
            aggQuotes := Quotes{}
            for key, value := range tiiex.indices {
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
                                        if quote.Epoch[bar] == aggQuote.Epoch[bar] {
                                            // Shallow Iteration on quote matches with aggQuote
                                            matchedEpochs = true
                                            matchedBar = bar
                                        } else {
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
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLCV")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("IEX: %v row(s) to %s/%s/OHLCV from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
            }
        }
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiiex.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 3, 0, time.UTC)
            // Check if timeEnd is Closing, will return Opening if so
            openTime := alignTimeToTradingHours(timeEnd, calendar)
            if openTime != timeEnd {
                // Set to wait till Opening
                waitTill = openTime
            }
            log.Info("IEX: Next request at %v", waitTill)
			time.Sleep(waitTill.Sub(time.Now().UTC()))
		} else {
			time.Sleep(time.Second*60)
		}
	}
}

func main() {
}
