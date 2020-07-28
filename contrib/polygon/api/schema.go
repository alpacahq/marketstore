package api

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

// AggResult is the structure that defines the actual Aggregate result
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
	DbLatency    int         `json:"db_latency"`
	// `map` is excluded as it only contains variable shortening info for ticks
}

// TradeTick is the structure that contains the actual
// tick data included in a HistoricTrades response
type TradeTick struct {
	ParticipantTimestamp int64   `json:"y"` // Participant/Exchange timestamp
	TrfTimestamp         int64   `json:"f"`
	SipTimestamp         int64   `json:"t"` // Optional
	Price                float64 `json:"p"`
	Size                 int     `json:"s"`
	Exchange             int     `json:"x"`
	Conditions           []int   `json:"c"`
	Id                   string  `json:"i"`
	Correction           int     `json:"e"`
	SequenceNumber       int     `json:"q"`
	TrfId                int     `json:"r"`
	Tape                 int     `json:"z"`
	OrigId               string  `string:"I"`
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
