package api

/*
Streaming Schema from Polygon

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
Streaming data.
*/

type PolyTrade struct {
	// eventType string `json:"-"` // ev
	Symbol string `json:"sym"`
	// exchange   int     `json:"-"` // x
	Price      float64 `json:"p"`
	Size       int     `json:"s"`
	Timestamp  int64   `json:"t"`
	Conditions []int   `json:"c"`
}

type PolyQuote struct {
	// eventType   string  `json:"-"` //ev
	Symbol string `json:"sym"`
	// bidExchange int     `json:"-"`
	BidPrice float64 `json:"bp"`
	BidSize  int     `json:"bs"`
	// askExchange int     `json:"-"`
	AskPrice float64 `json:"ap"`
	AskSize  int     `json:"as"`
	// condition   int     `json:"-"`
	Timestamp int64 `json:"t"`
}

type PolyAggregate struct {
	// eventType    string  `json:"-"` //ev
	Symbol string `json:"sym"`
	Volume int    `json:"v"`
	// accumVolume  int     `json:"-"`
	// officialOpen float64 `json:"-"`
	// vWAP         float64 `json:"-"`
	Open        float64 `json:"o"`
	Close       float64 `json:"c"`
	High        float64 `json:"h"`
	Low         float64 `json:"l"`
	EpochMillis int64   `json:"s"`
	// endTime      int64   `json:"-"`
}

/*
Historical data
*/

// HistoricAggregates is the structure that defines
// aggregate data served through polygon's REST API.
type HistoricAggregates struct {
	Ticker      string      `json:"ticker"`
	Status      string      `json:"status"`
	Adjusted    bool        `json:"adjusted"`
	QueryCount  int         `json:"queryCount"`
	ResultCount int         `json:"resultCount"`
	Results     []AggResult `json:"results"`
}

// AggResult is the structure that defines the actual Aggregate result.
type AggResult struct {
	// Volume should be int but json.Decode fails with: "cannot unmarshal number 1.70888e+06 into Go struct"
	Volume            float64 `json:"v"`
	Open              float64 `json:"o"`
	Close             float64 `json:"c"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	EpochMilliseconds int64   `json:"t"`
	NumberOfItems     int     `json:"n"`
}

// HistoricTrades is the structure that defines trade
// data served through polygon's REST API.
type HistoricTrades struct {
	Ticker       string      `json:"ticker"`
	Success      bool        `json:"success"`
	ResultsCount int         `json:"results_count"`
	Results      []TradeTick `json:"results"`
	DBLatency    int         `json:"db_latency"`
	// `map` is excluded as it only contains variable shortening info for ticks
}

// TradeTick is the structure that contains the actual
// tick data included in a HistoricTrades response.
type TradeTick struct {
	ParticipantTimestamp int64   `json:"y"` // Participant/Exchange timestamp
	TrfTimestamp         int64   `json:"f"`
	SIPTimestamp         int64   `json:"t"` // Optional
	Price                float64 `json:"p"`
	Size                 int     `json:"s"`
	Exchange             int     `json:"x"`
	Conditions           []int   `json:"c"`
	ID                   string  `json:"i"`
	Correction           int     `json:"e"`
	SequenceNumber       int     `json:"q"`
	TrfID                int     `json:"r"`
	Tape                 int     `json:"z"`
	OrigID               string  `string:"I"`
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
// tick data included in a HistoricQuotes response.
type QuoteTick struct {
	Timestamp   int64   `json:"t"`
	BidExchange int     `json:"bE"`
	AskExchange int     `json:"aE"`
	BidPrice    float64 `json:"bP"`
	AskPrice    float64 `json:"aP"`
	BidSize     int     `json:"bS"`
	AskSize     int     `json:"aS"`
	Condition   int     `json:"c"`
	Tape        int     `json:"z"`
}
