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

	if err != nil {
		log.Info("IEX: symbol '%s' error: %s \n %s", symbol, err, api_url)
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
        if ( ( !realTime && calendar.IsWorkday(from) && calendar.IsWorkday(to) ) || ( realTime && calendar.IsWorkday(from) && ( ( from.Hour() >= 12 ) && ( ( from.Hour() < 22 ) || ( from.Hour() == 22 && from.Minute() <= 30 ) ) ) ) ) {
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

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	US_EQ          []string  `json:"US_EQ"`
	US_CB          []string  `json:"US_CB"`
	US_GB          []string  `json:"US_GB"`
	US_FX          []string  `json:"US_FX"`
	EU_EQ          []string  `json:"EU_EQ"`
	EU_EQH         []string  `json:"EU_EQH"`
	EU_FX          []string  `json:"EU_FX"`
	GB_EQ          []string  `json:"GB_EQ"`
	GB_EQH         []string  `json:"GB_EQH"`
	GB_FX          []string  `json:"GB_FX"`
	JP_EQ          []string  `json:"JP_EQ"`
	JP_EQH         []string  `json:"JP_EQH"`
	JP_FX          []string  `json:"JP_FX"`
	CH_EQ          []string  `json:"CH_EQ"`
	CH_EQH         []string  `json:"CH_EQH"`
	CH_FX          []string  `json:"CH_FX"`
	AU_EQ          []string  `json:"AU_EQ"`
	AU_EQH         []string  `json:"AU_EQH"`
	AU_FX          []string  `json:"AU_FX"`
	CA_EQ          []string  `json:"CA_EQ"`
	CA_EQH         []string  `json:"CA_EQH"`
	CA_FX          []string  `json:"CA_FX"`
	CN_EQ          []string  `json:"CN_EQ"`
	CN_EQH         []string  `json:"CN_EQH"`
	CN_FX          []string  `json:"CN_FX"`
	EM_EQ          []string  `json:"EM_EQ"`
	EM_EQH         []string  `json:"EM_EQH"`
	EM_CB          []string  `json:"EM_CB"`
	EM_GB          []string  `json:"EM_GB"`
	EM_GBH         []string  `json:"EM_GBH"`
	EM_FX          []string  `json:"EM_FX"`
	DM_EQ          []string  `json:"DM_EQ"`
	DM_EQH         []string  `json:"DM_EQH"`
	DM_CB          []string  `json:"DM_CB"`
	DM_GB          []string  `json:"DM_GB"`
	DM_GBH         []string  `json:"DM_GBH"`
	DM_FX          []string  `json:"DM_FX"`
    ApiKey         string    `json:"api_key"`
	QueryStart     string    `json:"query_start"`
	BaseTimeframe  string    `json:"base_timeframe"`
}

// IEXFetcher is the main worker for TiingoIEX
type IEXFetcher struct {
	config         map[string]interface{}
	symbols        map[string][]string
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

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

    symbols := map[string][]string{
        "US-EQ": config.US_EQ,
        "US-CB": config.US_CB,
        "US-GB": config.US_GB,
        "US-FX": config.US_FX,
        "EU-EQ": config.EU_EQ,
        "EU-EQH": config.EU_EQH,
        "EU-FX": config.EU_FX,
        "GB-EQ": config.GB_EQ,
        "GB-EQH": config.GB_EQH,
        "GB-FX": config.GB_FX,
        "JP-EQ": config.JP_EQ,
        "JP-EQH": config.JP_EQH,
        "JP-FX": config.JP_FX,
        "CH-EQ": config.CH_EQ,
        "CH-EQH": config.CH_EQH,
        "CH-FX": config.CH_FX,
        "AU-EQ": config.AU_EQ,
        "AU-EQH": config.AU_EQH,
        "AU-FX": config.AU_FX,
        "CA-EQ": config.CA_EQ,
        "CA-EQH": config.CA_EQH,
        "CA-FX": config.CA_FX,
        "CN-EQ": config.CN_EQ,
        "CN-EQH": config.CN_EQH,
        "CN-FX": config.CN_FX,
        "EM-EQ": config.EM_EQ,
        "EM-EQH": config.EM_EQH,
        "EM-CB": config.EM_CB,
        "EM-GB": config.EM_GB,
        "EM-GBH": config.EM_GBH,
        "EM-FX": config.EM_FX,
        "DM-EQ": config.DM_EQ,
        "DM-EQH": config.DM_EQH,
        "DM-CB": config.DM_CB,
        "DM-GB": config.DM_GB,
        "DM-GBH": config.DM_GBH,
        "DM-FX": config.DM_FX,
    }
    
	return &IEXFetcher{
		config:         conf,
		symbols:        symbols,
        apiKey:         config.ApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiiex *IEXFetcher) Run() {

    symbols := make([]string, 0)
    for _, indSymbols := range tiiex.symbols {
        for _, symbol := range indSymbols {
            symbols = append(symbols, symbol)
        }
    }
    
	realTime := false    
	timeStart := time.Time{}
	lastTimestamp := time.Time{}
    
    // Get last timestamp collected
	for _, symbol := range symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
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
        
        log.Info("IEX: %v-%v", timeStart, timeEnd)
        
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
            
            rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
            // Data for symbols are retrieved in random order for fairness
            // Data for symbols are written immediately for asynchronous-like processing
            for _, symbol := range symbols {
                time.Sleep(300 * time.Millisecond)
                time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
                quote, err := GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiiex.baseTimeframe, calendar, tiiex.apiKey)
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
                    lastTimestamp = time.Unix(quote.Epoch[len(quote.Epoch)-1], 0)
                    log.Info("IEX: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
                    quotes = append(quotes, quote)
                } else {
                    log.Info("IEX: error downloading " + symbol)
                }
            }
            
            aggQuotes := Quotes{}
            for key, value := range tiiex.symbols {
                aggQuote := NewQuote(key, 0)
                for _, quote := range quotes {
                    for _, symbol := range value {
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
                tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiiex.baseTimeframe.String + "/OHLC")
                csm.AddColumnSeries(*tbk, cs)
                executor.WriteCSM(csm, false)
                
                log.Info("IEX: %v row(s) to %s/%s/OHLC from %v to %v", len(quote.Epoch), quote.Symbol, tiiex.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC())
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
