package api

import "github.com/alpacahq/marketstore/v4/contrib/alpaca/enums"

type Message struct {
	EventType enums.Prefix `json:"ev"`
}

type AlpacaMessage struct {
	Data Message `json:"data"`
}

// Trade is a trade
type Trade struct {
	// event name, always “T”
	eventType string `json:"-"` // ev
	// symbol
	Symbol string `json:"T"`
	// trade ID
	tradeID int `json:"-"` // i
	// exchange code where the trade occurred
	Exchange byte `json:"x"`
	// trade price
	Price float64 `json:"p"`
	// trade size (shares)
	Size int64 `json:"s"`
	// epoch timestamp in nanoseconds
	Timestamp int64 `json:"t"`
	// condition flags
	Conditions []byte `json:"c"`
	// tape ID
	TapeID byte `json:"z"`
}

// AlpacaTrade is the message
// from Alpaca that contains the trade
type AlpacaTrade struct {
	Data Trade `json:"data"`
}

// Quote is a quote
type Quote struct {
	// event name, always “Q”
	eventType string `json:"-"` // ev
	// symbol
	Symbol string `json:"T"`
	// exchange code for bid quote
	BidExchange byte `json:"x"`
	// bid price
	BidPrice float64 `json:"p"`
	// bid size
	BidSize int64 `json:"s"`
	// exchange code for ask quote
	AskExchange byte `json:"X"`
	// ask price
	AskPrice float64 `json:"P"`
	// ask size
	AskSize int64 `json:"S"`
	// condition flags.
	// NOTE: always has len = 1
	Conditions []int8 `json:"c"`
	// epoch timestamp in nanoseconds
	Timestamp int64 `json:"t"`
}

// AlpacaQuote is the message
// from Alpaca that contains the quote
type AlpacaQuote struct {
	Data Quote `json:"data"`
}

// AggregateToMinute is a minute aggregate
type AggregateToMinute struct {
	// event name, always “AM”
	eventType string `json:"-"` // ev
	// symbol
	Symbol string `json:"T"`
	// volume (shares)
	Volume int64 `json:"v"`
	// accumulated volume (shares)
	accumVolume int64 `json:"-"` // av
	//official open price of the bar
	officialOpen float64 `json:"-"` // op
	// VWAP (Volume Weighted Average Price)
	VWAP float64 `json:"vw"`
	// open price of the bar
	Open float64 `json:"o"`
	// close price of the bar
	Close float64 `json:"c"`
	// high price of the bar
	High float64 `json:"h"`
	// low price of the bar
	Low float64 `json:"l"`
	// average price of the bar
	Average float64 `json:"a"`
	// epoch time at the beginning of the window in milliseconds
	EpochMillis int64 `json:"s"`
	// epoch time at the ending of the window in milliseconds
	EndTime int64 `json:"e"`
}

// AlpacaAggregateToMinute is the message
// from Alpaca that contains the minute aggregate
type AlpacaAggregateToMinute struct {
	Data AggregateToMinute `json:"data"`
}
