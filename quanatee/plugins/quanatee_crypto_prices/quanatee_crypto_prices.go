package main

import (
	"encoding/json"
	"fmt"
	"math"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
    // "strings"
    "errors"
    "math/rand"
    // "math/big"
    
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
    
	"gopkg.in/yaml.v2"
)

// Quote - stucture for historical price data
type Quote struct {
	Symbol    string      `json:"symbol"`
	Precision int64       `json:"-"`
	Epoch     []int64     `json:"epoch"`
	Open      []float32   `json:"open"`
	High      []float32   `json:"high"`
	Low       []float32   `json:"low"`
	Close     []float32   `json:"close"`
	HLC       []float32   `json:"HLC"`
	Volume    []float32   `json:"volume"`
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
		Open:   make([]float32, bars),
		High:   make([]float32, bars),
		Low:    make([]float32, bars),
		Close:  make([]float32, bars),
		HLC:    make([]float32, bars),
		Volume: make([]float32, bars),
	}
}

func GetPolygonPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, token string) (Quote, error) {
    
	resampleFreq := "5"
	switch period.String {
	case "1Min":
		resampleFreq = "1"
	case "3Min":
		resampleFreq = "3"
	case "5Min":
		resampleFreq = "5"
	case "15Min":
		resampleFreq = "15"
	case "30Min":
		resampleFreq = "30"
	}
    
	type priceData struct {
        Ticker         string  `json:"T"`
		Volume         float32 `json:"v"`
		Open           float32 `json:"o"`
		High           float32 `json:"h"`
		Low            float32 `json:"l"`
		Close          float32 `json:"c"`
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
    
    var cryptoData polygonData
    // https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2019-01-01/2019-02-01?unadjusted=true&apiKey=
    apiUrl := fmt.Sprintf(
                        "https://api.polygon.io/v2/aggs/ticker/%s/range/%s/minute/%s/%s?unadjusted=false&apiKey=%s",
                        "X:"+symbol,
                        resampleFreq,
                        url.QueryEscape(from.AddDate(0, 0, -1).Format("2006-01-02")),
                        url.QueryEscape(to.Format("2006-01-02")),
                        token)
    
    if !realTime {
        time.Sleep(time.Millisecond*time.Duration(rand.Intn(25)))
    }
    
	client := &http.Client{Timeout: ClientTimeout}
	req, _ := http.NewRequest("GET", apiUrl, nil)
	//req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := client.Do(req)
    
    // Try again if fail
	if err != nil {
        time.Sleep(3 * time.Second)    
        resp, err = client.Do(req)
    }
    
	if err != nil {
		log.Warn("Crypto: Polygon symbol '%s' error: %s url: %s", symbol, err, apiUrl)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &cryptoData)
	if err != nil {
		//log.Warn("Crypto: Polygon symbol '%s' error: %v", symbol, err)
		log.Warn("Crypto: Polygon symbol '%s' error: %v", symbol, err)
		return NewQuote(symbol, 0), err
    }
    
	if len(cryptoData.PriceData) < 1 {
		return NewQuote(symbol, 0), err
	}
    
	numrows := len(cryptoData.PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt := time.Unix(0, cryptoData.PriceData[bar].Timestamp * int64(1000000)) //Timestamp is in Millisecond
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = cryptoData.PriceData[bar].Open
            quote.High[bar] = cryptoData.PriceData[bar].High
            quote.Low[bar] = cryptoData.PriceData[bar].Low
            quote.Close[bar] = cryptoData.PriceData[bar].Close
            quote.HLC[bar] = (cryptoData.PriceData[bar].High + cryptoData.PriceData[bar].Low + cryptoData.PriceData[bar].Close)/3
            quote.Volume[bar] = cryptoData.PriceData[bar].Volume
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

func GetTiingoPrices(symbol string, from, to, last time.Time, realTime bool, period *utils.Timeframe, token string) (Quote, error) {
    
	resampleFreq := "1hour"
	switch period.String {
	case "1Min":
		resampleFreq = "1min"
	case "3Min":
		resampleFreq = "3min"
	case "5Min":
		resampleFreq = "5min"
	case "15Min":
		resampleFreq = "15min"
	case "30Min":
		resampleFreq = "30min"
	}

	type priceData struct {
		TradesDone     float32 `json:"tradesDone"`
		Close          float32 `json:"close"`
		VolumeNotional float32 `json:"volumeNotional"`
		Low            float32 `json:"low"`
		Open           float32 `json:"open"`
		Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
		High           float32 `json:"high"`
		Volume         float32 `json:"volume"`
	}

	type tiingoData struct {
		Ticker        string      `json:"ticker"`
		BaseCurrency  string      `json:"baseCurrency"`
		QuoteCurrency string      `json:"quoteCurrency"`
		PriceData     []priceData `json:"priceData"`
	}

	var cryptoData []tiingoData

    apiUrl := fmt.Sprintf(
                        "https://api.tiingo.com/tiingo/crypto/prices?tickers=%s&resampleFreq=%s&startDate=%s",
                        symbol,
                        resampleFreq,
                        url.QueryEscape(from.Format("2006-1-2")))
    
    if !realTime {
        apiUrl = apiUrl + "&endDate=" + url.QueryEscape(to.Format("2006-1-2"))
        time.Sleep(time.Millisecond*time.Duration(rand.Intn(25)))
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
		log.Warn("Crypto: symbol '%s' error: %s \n %s", symbol, err, apiUrl)
		return NewQuote(symbol, 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(contents, &cryptoData)
	if err != nil {
		log.Warn("Crypto: Tiingo symbol '%s' error: %v", symbol, err)
		return NewQuote(symbol, 0), err
	}
	if len(cryptoData) < 1 {
		return NewQuote(symbol, 0), err
	}

	numrows := len(cryptoData[0].PriceData)
	quote := NewQuote(symbol, numrows)
    // Pointers to help slice into just the relevent datas
    startOfSlice := -1
    endOfSlice := -1
    
	for bar := 0; bar < numrows; bar++ {
        dt, _ := time.Parse(time.RFC3339, cryptoData[0].PriceData[bar].Date)
        // Only add data collected between from (timeStart) and to (timeEnd) range to prevent overwriting or confusion when aggregating data
        if dt.UTC().Unix() > last.UTC().Unix() && dt.UTC().Unix() >= from.UTC().Unix() && dt.UTC().Unix() <= to.UTC().Unix() {
            if startOfSlice == -1 {
                startOfSlice = bar
            }
            endOfSlice = bar
            quote.Epoch[bar] = dt.UTC().Unix()
            quote.Open[bar] = cryptoData[0].PriceData[bar].Open
            quote.High[bar] = cryptoData[0].PriceData[bar].High
            quote.Low[bar] = cryptoData[0].PriceData[bar].Low
            quote.Close[bar] = cryptoData[0].PriceData[bar].Close
            quote.HLC[bar] = (quote.High[bar] + quote.Low[bar] + quote.Close[bar])/3
            quote.Volume[bar] = float32(cryptoData[0].PriceData[bar].Volume)
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
    TiingoApiKey   string   `yaml:"tiingo_api_key"`
    PolygonApiKey  string   `yaml:"polygon_api_key"`
	QueryStart     string   `yaml:"query_start"`
	BaseTimeframe  string   `yaml:"base_timeframe"`
}

// CryptoFetcher is the main worker for TiingoCrypto
type CryptoFetcher struct {
	config         map[string]interface{}
	symbols        []string
    tiingoApiKey   string
    polygonApiKey  string
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

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}
    
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}

    if config.TiingoApiKey == "<tiingo_api_key>" {
        config.TiingoApiKey = ""
    }
    
    if config.PolygonApiKey == "<polygon_api_key>" {
        config.PolygonApiKey = ""
    }
    
	return &CryptoFetcher{
		config:         conf,
		symbols:        symbols,
        tiingoApiKey:   config.TiingoApiKey,
        polygonApiKey:  config.PolygonApiKey,
		queryStart:     queryStart,
		baseTimeframe:  utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (tiicc *CryptoFetcher) Run() {
    
	realTime := false
    
    timeStart := tiicc.queryStart.UTC()
	lastTimestamp := time.Time{}
	
    // Get last timestamp collected
	for _, symbol := range tiicc.symbols {
        tbk := io.NewTimeBucketKey(symbol + "/" + tiicc.baseTimeframe.String + "/Price")
        lastTimestamp = findLastTimestamp(tbk)
        log.Info("Crypto: lastTimestamp for %s = %v", symbol, lastTimestamp)
        if !lastTimestamp.IsZero() && lastTimestamp.After(timeStart) {
            timeStart = lastTimestamp.UTC()
        }
	}
    
	// For loop for collecting candlestick data forever
	var timeEnd time.Time
	firstLoop := true

	for {
        
        if firstLoop {
            firstLoop = false
        } else {
            timeStart = timeEnd
        }
        if realTime {
            // Add timeEnd by a tick
            timeEnd = timeStart.Add(tiicc.baseTimeframe.Duration)
        } else {
            // Add timeEnd by a range
            timeEnd = timeStart.AddDate(0, 0, 3)
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
        
        var quotes []Quote
        symbols := tiicc.symbols
        rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })
        // Data for symbols are retrieved in random order for fairness
        // Data for symbols are written immediately for asynchronous-like processing
        written := []string{}
        unwritten := []string{}
        for _, symbol := range symbols {
            tiingoQuote := NewQuote(symbol, 0)
            var tiingoErr error
            if tiicc.tiingoApiKey != "" {
                tiingoQuote, tiingoErr = GetTiingoPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiicc.baseTimeframe, tiicc.tiingoApiKey)
            } else {
                tiingoErr = errors.New("No api key")
            }
            polygonQuote := NewQuote(symbol, 0)
            var polygonErr error
            if tiicc.polygonApiKey != "" {
                polygonQuote, polygonErr = GetPolygonPrices(symbol, timeStart, timeEnd, lastTimestamp, realTime, tiicc.baseTimeframe, tiicc.polygonApiKey)
            } else {
                polygonErr = errors.New("No api key")
            }
            quote := NewQuote(symbol, 0)
            dataProvider := "None"
            if len(polygonQuote.Epoch) == len(tiingoQuote.Epoch) && (tiingoErr == nil && polygonErr == nil) {
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
            } else if (len(polygonQuote.Epoch) > 0 && len(tiingoQuote.Epoch) > 0) && (tiingoErr == nil && polygonErr == nil) {
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
                    if len(quote2.Epoch) > bar && quote.Epoch[bar] == quote2.Epoch[bar] {
                            quote.Open[bar] = (quote.Open[bar] + quote2.Open[bar]) / 2
                            quote.High[bar] = (quote.High[bar] + quote2.High[bar]) / 2
                            quote.Low[bar] = (quote.Low[bar] + quote2.Low[bar]) / 2
                            quote.Close[bar] = (quote.Close[bar] + quote2.Close[bar]) / 2
                            quote.HLC[bar] = (quote.HLC[bar] + quote2.HLC[bar]) / 2
                            quote.Volume[bar] = (quote.Volume[bar] + quote2.Volume[bar])
                    } else {
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
                }
                dataProvider = "Odd Aggregation"
            } else if (len(polygonQuote.Epoch) > 0 && polygonQuote.Epoch[0] > 0 && polygonQuote.Epoch[len(polygonQuote.Epoch)-1] > 0) || (tiingoErr != nil && polygonErr == nil) {
                // Only one quote is valid
                quote = polygonQuote
                dataProvider = "Polygon"
            } else if (len(tiingoQuote.Epoch) > 0 && tiingoQuote.Epoch[0] > 0 && tiingoQuote.Epoch[len(tiingoQuote.Epoch)-1] > 0) || (tiingoErr == nil && polygonErr != nil) {  
                // Only one quote is valid
                quote = tiingoQuote
                dataProvider = "Tiingo"
            }
            
            if len(quote.Epoch) < 1 {
                // Check if there is data to add
                unwritten = append(unwritten, symbol)
                continue
            } else if realTime && lastTimestamp.Unix() >= quote.Epoch[0] && lastTimestamp.Unix() >= quote.Epoch[len(quote.Epoch)-1] {
                // Check if realTime is adding the most recent data
                log.Info("Crypto: Previous row dated %v is still the latest in %s/%s/Price \n", time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), quote.Symbol, tiicc.baseTimeframe.String)
                unwritten = append(unwritten, symbol)
                continue
            } else if dataProvider == "None" {
                unwritten = append(unwritten, symbol)
                continue
            } else {
                if realTime && len(quote.Epoch) > 1 {
                    // write to csm
                    cs := io.NewColumnSeries()
                    cs.AddColumn("Epoch", []int64{quote.Epoch[len(quote.Epoch)-1]})
                    cs.AddColumn("Open", []float32{quote.Open[len(quote.Epoch)-1]})
                    cs.AddColumn("High", []float32{quote.High[len(quote.Epoch)-1]})
                    cs.AddColumn("Low", []float32{quote.Low[len(quote.Epoch)-1]})
                    cs.AddColumn("Close", []float32{quote.Close[len(quote.Epoch)-1]})
                    cs.AddColumn("HLC", []float32{quote.HLC[len(quote.Epoch)-1]})
                    cs.AddColumn("Volume", []float32{quote.Volume[len(quote.Epoch)-1]})
                    csm := io.NewColumnSeriesMap()
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/Price")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    log.Info("Crypto: 1 (%v) row(s) to %s/%s/Price from %v to %v by %s ", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
                } else {
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
                    tbk := io.NewTimeBucketKey(quote.Symbol + "/" + tiicc.baseTimeframe.String + "/Price")
                    csm.AddColumnSeries(*tbk, cs)
                    executor.WriteCSM(csm, false)
                    log.Info("Crypto: %v row(s) to %s/%s/Price from %v to %v by %s ", len(quote.Epoch), quote.Symbol, tiicc.baseTimeframe.String, time.Unix(quote.Epoch[0], 0).UTC(), time.Unix(quote.Epoch[len(quote.Epoch)-1], 0).UTC(), dataProvider)
                }
                quotes = append(quotes, quote)
                written = append(written, symbol)
            }
        }
        
        log.Info("Crypto Written: %v", written)
        log.Info("Crypto Not Written: %v", unwritten)

        // Save the latest timestamp written
        if len(quotes) > 0 {
            if len(quotes[0].Epoch) > 0{
                lastTimestamp = time.Unix(quotes[0].Epoch[len(quotes[0].Epoch)-1], 0)
            }
        }
        
        if realTime {
            for {
                if time.Now().UTC().Unix() > timeEnd.Add(tiicc.baseTimeframe.Duration).UTC().Unix() {
                    break
                } else {
                    oneMinuteAhead := time.Now().Add(time.Minute)
                    oneMinuteAhead = time.Date(oneMinuteAhead.Year(), oneMinuteAhead.Month(), oneMinuteAhead.Day(), oneMinuteAhead.Hour(), oneMinuteAhead.Minute(), 0, 0, time.UTC)
                    time.Sleep(oneMinuteAhead.UTC().Sub(time.Now().UTC()))
                }
            }
        } else {
			time.Sleep(time.Millisecond*time.Duration(rand.Intn(25)))
        }

	}
}

func main() {
}
