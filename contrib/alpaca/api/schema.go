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
	Exchange uint32 `json:"x"`
	// trade price
	Price float32 `json:"p"`
	// trade size (shares)
	Size uint32 `json:"s"`
	// epoch timestamp in nanoseconds
	Timestamp int64 `json:"t"`
	// condition flags
	Conditions []int32 `json:"c"`
	// tape ID
	TapeID uint32 `json:"z"`
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
	BidExchange uint32 `json:"x"`
	// bid price
	BidPrice float32 `json:"p"`
	// bid size
	BidSize uint32 `json:"s"`
	// exchange code for ask quote
	AskExchange uint32 `json:"X"`
	// ask price
	AskPrice float32 `json:"P"`
	// ask size
	AskSize uint32 `json:"S"`
	// condition flags.
	// NOTE: always has len = 1
	Conditions []int32 `json:"c"`
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
	Volume uint32 `json:"v"`
	// accumulated volume (shares)
	accumVolume int `json:"-"` // av
	//official open price of the bar
	officialOpen float32 `json:"-"` // op
	// VWAP (Volume Weighted Average Price)
	VWAP float32 `json:"vw"`
	// open price of the bar
	Open float32 `json:"o"`
	// close price of the bar
	Close float32 `json:"c"`
	// high price of the bar
	High float32 `json:"h"`
	// low price of the bar
	Low float32 `json:"l"`
	// average price of the bar
	Average float32 `json:"a"`
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
