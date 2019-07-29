package api

/*
Streaming Schema from Polygon
// Stocks TRADE:
{
    "ev": "T",              // Event Type
    "sym": "MSFT",          // Symbol Ticker
    "x": "4",               // Exchange ID
    "p": 114.125,           // Price
    "s": 100,               // Trade Size
    "c": [0, 12],           // Trade Conditions
    "t": 1536036818784      // Trade Timestamp ( Unix MS )
}

// Stocks QUOTE:
{
    "ev": "Q",              // Event Type
    "sym": "MSFT",          // Symbol Ticker
    "bx": "4",              // Bix Exchange ID
    "bp": 114.125,          // Bid Price
    "bs": 100,              // Bid Size
    "ax": "7",              // Ask Exchange ID
    "ap": 114.128,          // Ask Price
    "as": 160,              // Ask Size
    "c": 0,                 // Quote Condition
    "t": 1536036818784      // Quote Timestamp ( Unix MS )
}

// Stocks Aggregate:
{
    "ev": "AM",             // Event Type ( A = Second Agg, AM = Minute Agg )
    "sym": "MSFT",          // Symbol Ticker
    "v": 10204,             // Tick Volume
    "av": 200304,           // Accumlated Volume ( Today )
    "op": 114.04,           // Todays official opening price
    "vw": 114.4040,         // VWAP (Volume Weighted Average Price)
    "o": 114.11,            // Tick Open Price
    "c": 114.14,            // Tick Close Price
    "h": 114.19,            // Tick High Price
    "l": 114.09,            // Tick Low Price
    "a": 114.1314,          // Tick Average / VWAP Price
    "s": 1536036818784,     // Tick Start Timestamp ( Unix MS )
    "e": 1536036818784,     // Tick End Timestamp ( Unix MS )
}
*/
/*
Streaming data
*/
type PolyTrade struct {
	eventType  string  `json:"-"` //ev
	Symbol     string  `json:"sym"`
	exchange   int     `json:"-"` //x
	Price      float64 `json:"p"`
	Size       int64   `json:"s"`
	Timestamp  int64   `json:"t"`
	Conditions []int   `json:"c"`
}

type PolyQuote struct {
	eventType   string  `json:"-"` //ev
	Symbol      string  `json:"sym"`
	bidExchange int     `json:"-"`
	BidPrice    float64 `json:"bp"`
	BidSize     int64   `json:"bs"`
	askExchange int     `json:"-"`
	AskPrice    float64 `json:"ap"`
	AskSize     int64   `json:"as"`
	condition   int     `json:"-"`
	Timestamp   int64   `json:"t"`
}

type PolyAggregate struct {
	eventType    string  `json:"-"` //ev
	Symbol       string  `json:"sym"`
	Volume       int     `json:"v"`
	accumVolume  int     `json:"-"`
	officialOpen float64 `json:"-"`
	vWAP         float64 `json:"-"`
	Open         float64 `json:"o"`
	Close        float64 `json:"c"`
	High         float64 `json:"h"`
	Low          float64 `json:"l"`
	EpochMillis  int64   `json:"s"`
	endTime      int64   `json:"-"`
}

/*
Historical data
*/

// HistoricAggregates is the structure that defines
// aggregate data served through polygon's REST API.
type HistoricAggregates struct {
	Symbol        string `json:"symbol"`
	AggregateType string `json:"aggType"`
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

// AggTick is the structure that contains the actual
// tick data included in a HistoricAggregates response
type AggTick struct {
	EpochMilliseconds int64   `json:"d"`
	Open              float64 `json:"o"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	Close             float64 `json:"c"`
	Volume            int     `json:"v"`
}

// HistoricTrades is the structure that defines trade
// data served through polygon's REST API.
type HistoricTrades struct {
	Day string `json:"day"`
	Map struct {
		C1 string `json:"c1"`
		C2 string `json:"c2"`
		C3 string `json:"c3"`
		C4 string `json:"c4"`
		E  string `json:"e"`
		P  string `json:"p"`
		S  string `json:"s"`
		T  string `json:"t"`
	} `json:"map"`
	MsLatency int         `json:"msLatency"`
	Status    string      `json:"status"`
	Symbol    string      `json:"symbol"`
	Ticks     []TradeTick `json:"ticks"`
	Type      string      `json:"type"`
}

// TradeTick is the structure that contains the actual
// tick data included in a HistoricTrades response
type TradeTick struct {
	Timestamp  int64   `json:"t"`
	Price      float64 `json:"p"`
	Size       int     `json:"s"`
	Exchange   string  `json:"e"`
	Condition1 int     `json:"c1"`
	Condition2 int     `json:"c2"`
	Condition3 int     `json:"c3"`
	Condition4 int     `json:"c4"`
}

// HistoricQuotes is the structure that defines quote
// data served through polygon's REST API.
type HistoricQuotes struct {
	Day string `json:"day"`
	Map struct {
		AE string `json:"aE"`
		AP string `json:"aP"`
		AS string `json:"aS"`
		BE string `json:"bE"`
		BP string `json:"bP"`
		BS string `json:"bS"`
		C  string `json:"c"`
		T  string `json:"t"`
	} `json:"map"`
	MsLatency int         `json:"msLatency"`
	Status    string      `json:"status"`
	Symbol    string      `json:"symbol"`
	Ticks     []QuoteTick `json:"ticks"`
	Type      string      `json:"type"`
}

// QuoteTick is the structure that contains the actual
// tick data included in a HistoricQuotes response
type QuoteTick struct {
	Timestamp   int64   `json:"t"`
	BidExchange string  `json:"bE"`
	AskExchange string  `json:"aE"`
	BidPrice    float64 `json:"bP"`
	AskPrice    float64 `json:"aP"`
	BidSize     int     `json:"bS"`
	AskSize     int     `json:"aS"`
	Condition   int     `json:"c"`
}
