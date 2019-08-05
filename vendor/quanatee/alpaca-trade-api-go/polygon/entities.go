package polygon

import "time"

// SymbolsMetadata is the structure that defines symbol
// metadata served through polygon's REST API.
type SymbolsMetadata struct {
	Symbols []struct {
		Symbol  string    `json:"symbol"`
		Name    string    `json:"name"`
		Type    string    `json:"type"`
		Updated time.Time `json:"updated"`
		IsOTC   bool      `json:"isOTC"`
		URL     string    `json:"url"`
	} `json:"symbols"`
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

type GetHistoricTradesParams struct {
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit"`
}

// AggTick is the structure that contains the actual
// tick data included in a HistoricAggregates response
type AggTick struct {
	Open              float64 `json:"o"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	Close             float64 `json:"c"`
	Volume            float64 `json:"v"`
	EpochMilliseconds int64   `json:"t"`
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

// polygon stream

// PolygonClientMsg is the standard message sent by clients of the stream interface
type PolygonClientMsg struct {
	Action string `json:"action"`
	Params string `json:"params"`
}

type PolygonAuthMsg struct {
	Event   string `json:"ev"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// PolygonServerMsg contains the field that is present in all responses to identify their type
type PolgyonServerMsg struct {
	Event string `json:"ev"`
}

// StreamTrade is the structure that defines a trade that
// polygon transmits via websocket protocol.
type StreamTrade struct {
	Symbol     string  `json:"sym"`
	Exchange   int     `json:"x"`
	Price      float64 `json:"p"`
	Size       int64   `json:"s"`
	Timestamp  int64   `json:"t"`
	Conditions []int   `json:"c"`
}

// StreamQuote is the structure that defines a quote that
// polygon transmits via websocket protocol.
type StreamQuote struct {
	Symbol      string  `json:"sym"`
	Condition   int     `json:"c"`
	BidExchange int     `json:"bx"`
	AskExchange int     `json:"ax"`
	BidPrice    float64 `json:"bp"`
	AskPrice    float64 `json:"ap"`
	BidSize     int64   `json:"bs"`
	AskSize     int64   `json:"as"`
	Timestamp   int64   `json:"t"`
}

// StreamAggregate is the structure that defines an aggregate that
// polygon transmits via websocket protocol.
type StreamAggregate struct {
	Event             string  `json:"ev"`
	Symbol            string  `json:"sym"`
	Volume            int     `json:"v"`
	AccumulatedVolume int     `json:"av"`
	OpeningPrice      float64 `json:"op"`
	VWAP              float64 `json:"vw"`
	OpenPrice         float64 `json:"o"`
	ClosePrice        float64 `json:"c"`
	HighPrice         float64 `json:"h"`
	LowPrice          float64 `json:"l"`
	Average           float64 `json:"a"`
	StartTimestamp    int64   `json:"s"`
	EndTimestamp      int64   `json:"e"`
}

// Exchange defines the Stocks / Equities "Exchange" endpoint response
type StockExchange struct {
	Id     int64  `json:"id"`
	Type   string `json:"type"`
	Market string `json:"market"`
	Mic    string `json:"mic"`
	Name   string `json:"name"`
	Tape   string `json:"tape"`
}
