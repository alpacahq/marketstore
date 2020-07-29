package streaming

import _ "github.com/mailru/easyjson/gen"

//go:generate easyjson $GOFILE

// Aggregate is the stock aggregate coming from Polygon
//easyjson:json
type Aggregate struct {
	EventType    string  `json:"ev"  msgpack:"ev"`
	Symbol       string  `json:"sym" msgpack:"sym"`
	Volume       int64   `json:"v"   msgpack:"v"`
	AccumVolume  int64   `json:"av"  msgpack:"av"`
	OfficialOpen float64 `json:"op"  msgpack:"op"`
	VWAP         float64 `json:"vw"  msgpack:"vw"`
	Open         float64 `json:"o"   msgpack:"o"`
	Close        float64 `json:"c"   msgpack:"c"`
	High         float64 `json:"h"   msgpack:"h"`
	Low          float64 `json:"l"   msgpack:"l"`
	Average      float64 `json:"a"   msgpack:"a"`
	EpochMillis  int64   `json:"s"   msgpack:"s"`
	EndTime      int64   `json:"e"   msgpack:"e"`
}

// Quote is the stock quote coming from Polygon
//easyjson:json
type Quote struct {
	EventType   string  `json:"ev"  msgpack:"ev"`
	Symbol      string  `json:"sym" msgpack:"sym"`
	BidExchange int32   `json:"bx"  msgpack:"bx"`
	BidPrice    float64 `json:"bp"  msgpack:"bp"`
	BidSize     int64   `json:"bs"  msgpack:"bs"`
	AskExchange int32   `json:"ax"  msgpack:"ax"`
	AskPrice    float64 `json:"ap"  msgpack:"ap"`
	AskSize     int64   `json:"as"  msgpack:"as"`
	Condition   int32   `json:"c"   msgpack:"c"`
	Timestamp   int64   `json:"t"   msgpack:"t"`
}

// Trade is the stock trade coming from Polygon
//easyjson:json
type Trade struct {
	EventType  string  `json:"ev"  msgpack:"ev"`
	Symbol     string  `json:"sym" msgpack:"sym"`
	ID         string  `json:"i"   msgpack:"i"`
	Exchange   int32   `json:"x"   msgpack:"x"`
	Price      float64 `json:"p"   msgpack:"p"`
	Size       int64   `json:"s"   msgpack:"s"`
	Timestamp  int64   `json:"t"   msgpack:"t"`
	Conditions []int32 `json:"c"   msgpack:"c"`
	Tape       int32   `json:"z"   msgpack:"z"`
}
