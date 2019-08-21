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
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
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

func GetTiingoPrices(symbol string, from, to time.Time, realTime bool, period string, token string) (Quote, error) {

	resampleFreq := "1hour"
	switch period {
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
    
	var iexData []priceData
    
    api_url := fmt.Sprintf(
                        "https://api.tiingo.com/iex/%s/prices?resampleFreq=%s&afterHours=true&forceFill=true&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    if !realTime {
        api_url = api_url + "&endDate=" + url.QueryEscape(to.Format("2006-1-2"))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", api_url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", token))
	resp, err := client.Do(req)

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
    
	if err != nil {
		log.Info("IEX: symbol '%s' not found\n", symbol)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &iexData)
	if err != nil {
		log.Info("IEX: symbol '%s' error: %v\n contents: %s", symbol, err, contents)
		return NewQuote(symbol, 0), err
	}
    
	if len(iexData) < 1 {
        if !calendar.IsWorkday(from) && !calendar.IsWorkday(to) {
            log.Warn("IEX: symbol '%s' No data returned from %v-%v, url %s", symbol, from, to, api_url)
        }
 		return NewQuote(symbol, 0), err
	}
    
	numrows := len(iexData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, iexData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = iexData[bar].Open
            quote.High[bar] = iexData[bar].High
            quote.Low[bar] = iexData[bar].Low
            quote.Close[bar] = iexData[bar].Close
        }
	}
    
    if startOfSlice > -1 && endOfSlice > -1 {
        quote.Epoch = quote.Epoch[startOfSlice:endOfSlice]
        quote.Open = quote.Open[startOfSlice:endOfSlice]
        quote.High = quote.High[startOfSlice:endOfSlice]
        quote.Low = quote.Low[startOfSlice:endOfSlice]
        quote.Close = quote.Close[startOfSlice:endOfSlice]
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
    ApiKey         string   `json:"api_key"`
	QueryStart     string   `json:"query_start"`
	BaseTimeframe  string   `json:"base_timeframe"`
	USTFSymbols    []string `json:"ustf_symbols"`
	EUTFSymbols    []string `json:"eutf_symbols"`
	JPTFSymbols    []string `json:"jptf_symbols"`
	WWTFSymbols    []string `json:"wwtf_symbols"`
	EMTFSymbols    []string `json:"emtf_symbols"`
}

// IEXFetcher is the main worker for TiingoIEX
type IEXFetcher struct {
	config         map[string]interface{}
	symbols        []string
    apiKey         string
	queryStart     time.Time
	baseTimeframe  *utils.Timeframe
	ustfSymbols    []string
	eutfSymbols    []string
	jptfSymbols    []string
	wwtfSymbols    []string
	emtfSymbols    []string
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

func alignTimeToTradingHours(timeCheck time.Time) time.Time {
    
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
            }
            days += days
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
	var ustfSymbols []string
	var eutfSymbols []string
	var jptfSymbols []string
	var wwtfSymbols []string
	var emtfSymbols []string

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
    
	if len(config.USTFSymbols) > 0 {
		ustfSymbols = config.USTFSymbols
	}
    
	if len(config.EUTFSymbols) > 0 {
		eutfSymbols = config.EUTFSymbols
	}
    
	if len(config.JPTFSymbols) > 0 {
		jptfSymbols = config.JPTFSymbols
	}
    
	if len(config.WWTFSymbols) > 0 {
		wwtfSymbols = config.WWTFSymbols
	}
	if len(config.EMTFSymbols) > 0 {
		emtfSymbols = config.EMTFSymbols
	}
	return &IEXFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
        ustfSymbols:    ustfSymbols,
        eutfSymbols:    eutfSymbols,
        jptfSymbols:    jptfSymbols,
        wwtfSymbols:    wwtfSymbols,
        emtfSymbols:    emtfSymbols,
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
        tbk := io.NewTimeBucketKey(symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("IEX: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// Set start time if not given.
	if !tiiex.queryStart.IsZero() {
		timeStart = tiiex.queryStart.UTC()
	} else {
		timeStart = time.Now().UTC()
	}
    timeStart = alignTimeToTradingHours(timeStart)
    
	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	var waitTill time.Time
	firstLoop := true
    
	for {
        
        if firstLoop {
            firstLoop = false
        } else {
            timeStart = lastTimestamp
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
                time.Sleep(250 * time.Millisecond)
                time.Sleep(time.Duration(rand.Intn(250)) * time.Millisecond)
                quote, err := GetTiingoPrices(symbol, timeStart, timeEnd, realTime, tiiex.baseTimeframe.String, tiiex.apiKey)
                if err == nil {
                    if len(quote.Epoch) < 1 {
                        // Check if there is data to add
                        continue
                    } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                        // Check if realTime is adding the most recent data
                        log.Info("IEX: Previous row dated %v is still the latest in %s/%s/OHLC", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiiex.baseTimeframe.String)
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
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    
                    // Save the latest timestamp written
                    lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC()
                    log.Info("IEX: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, quote)
                } else {
                    log.Info("IEX: error downloading " + symbol)
                }
            }
            
            aggQuotes := Quotes{}
            
            // Add USTF
            if len(tiiex.ustfSymbols) > 0 {
                ustf_quote := NewQuote("USTF", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiiex.ustfSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(ustf_quote.Epoch) == 0 {
                                    ustf_quote.Epoch = quote.Epoch
                                    ustf_quote.Open = quote.Open
                                    ustf_quote.High = quote.High
                                    ustf_quote.Low = quote.Low
                                    ustf_quote.Close = quote.Close
                                } else if len(ustf_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(ustf_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        ustf_quote.Open[bar] = (quote.Open[bar] + ustf_quote.Open[bar]) / 2
                                        ustf_quote.High[bar] = (quote.High[bar] + ustf_quote.High[bar]) / 2
                                        ustf_quote.Low[bar] = (quote.Low[bar] + ustf_quote.Low[bar]) / 2
                                        ustf_quote.Close[bar] = (quote.Close[bar] + ustf_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(ustf_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, ustf_quote)
                }
            }
            // Add EUTF
            if len(tiiex.eutfSymbols) > 0 {
                eutf_quote := NewQuote("EUTF", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiiex.eutfSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(eutf_quote.Epoch) == 0 {
                                    eutf_quote.Epoch = quote.Epoch
                                    eutf_quote.Open = quote.Open
                                    eutf_quote.High = quote.High
                                    eutf_quote.Low = quote.Low
                                    eutf_quote.Close = quote.Close
                                } else if len(eutf_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(eutf_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        eutf_quote.Open[bar] = (quote.Open[bar] + eutf_quote.Open[bar]) / 2
                                        eutf_quote.High[bar] = (quote.High[bar] + eutf_quote.High[bar]) / 2
                                        eutf_quote.Low[bar] = (quote.Low[bar] + eutf_quote.Low[bar]) / 2
                                        eutf_quote.Close[bar] = (quote.Close[bar] + eutf_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(eutf_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, eutf_quote)
                }
            }
            // Add JPTF
            if len(tiiex.jptfSymbols) > 0 {
                jptf_quote := NewQuote("JPTF", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiiex.jptfSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(jptf_quote.Epoch) == 0 {
                                    jptf_quote.Epoch = quote.Epoch
                                    jptf_quote.Open = quote.Open
                                    jptf_quote.High = quote.High
                                    jptf_quote.Low = quote.Low
                                    jptf_quote.Close = quote.Close
                                } else if len(jptf_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(jptf_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        jptf_quote.Open[bar] = (quote.Open[bar] + jptf_quote.Open[bar]) / 2
                                        jptf_quote.High[bar] = (quote.High[bar] + jptf_quote.High[bar]) / 2
                                        jptf_quote.Low[bar] = (quote.Low[bar] + jptf_quote.Low[bar]) / 2
                                        jptf_quote.Close[bar] = (quote.Close[bar] + jptf_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(jptf_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, jptf_quote)
                }
            }
            // Add WWTF
            if len(tiiex.wwtfSymbols) > 0 {
                wwtf_quote := NewQuote("WWTF", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiiex.wwtfSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(wwtf_quote.Epoch) == 0 {
                                    wwtf_quote.Epoch = quote.Epoch
                                    wwtf_quote.Open = quote.Open
                                    wwtf_quote.High = quote.High
                                    wwtf_quote.Low = quote.Low
                                    wwtf_quote.Close = quote.Close
                                } else if len(wwtf_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(wwtf_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        wwtf_quote.Open[bar] = (quote.Open[bar] + wwtf_quote.Open[bar]) / 2
                                        wwtf_quote.High[bar] = (quote.High[bar] + wwtf_quote.High[bar]) / 2
                                        wwtf_quote.Low[bar] = (quote.Low[bar] + wwtf_quote.Low[bar]) / 2
                                        wwtf_quote.Close[bar] = (quote.Close[bar] + wwtf_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(wwtf_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, wwtf_quote)
                }
            }
            // Add EMTF
            if len(tiiex.emtfSymbols) > 0 {
                emtf_quote := NewQuote("EMTF", 0)
                for _, quote := range quotes {
                    for _, symbol := range tiiex.emtfSymbols {
                        if quote.Symbol == symbol {
                            if len(quote.Epoch) > 0 {
                                if len(emtf_quote.Epoch) == 0 {
                                    emtf_quote.Epoch = quote.Epoch
                                    emtf_quote.Open = quote.Open
                                    emtf_quote.High = quote.High
                                    emtf_quote.Low = quote.Low
                                    emtf_quote.Close = quote.Close
                                } else if len(emtf_quote.Epoch) == len(quote.Epoch) {
                                    numrows := len(emtf_quote.Epoch)
                                    for bar := 0; bar < numrows; bar++ {
                                        emtf_quote.Open[bar] = (quote.Open[bar] + emtf_quote.Open[bar]) / 2
                                        emtf_quote.High[bar] = (quote.High[bar] + emtf_quote.High[bar]) / 2
                                        emtf_quote.Low[bar] = (quote.Low[bar] + emtf_quote.Low[bar]) / 2
                                        emtf_quote.Close[bar] = (quote.Close[bar] + emtf_quote.Close[bar]) / 2
                                    }
                                }
                            }
                        }
                    }
                }
                if len(emtf_quote.Epoch) > 0 {
                    aggQuotes = append(aggQuotes, emtf_quote)
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
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("IEX: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, timeStart, timeEnd)
            }
        }
		if realTime {
			// Sleep till next :00 time
            // This function ensures that we will always get full candles
			waitTill = time.Now().UTC().Add(tiiex.baseTimeframe.Duration)
            waitTill = time.Date(waitTill.Year(), waitTill.Month(), waitTill.Day(), waitTill.Hour(), waitTill.Minute(), 0, 0, time.UTC)
            // Check if timeEnd is Closing, will return Opening if so
            openTime := alignTimeToTradingHours(timeEnd)
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
