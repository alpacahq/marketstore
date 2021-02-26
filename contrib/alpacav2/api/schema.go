package api

import "time"

// Trade is a trade
type Trade struct {
	// event name, always “t”
	//	eventType string `mapstructure:"T"`
	// symbol
	Symbol string `mapstructure:"S"`
	// trade ID
	//tradeID int `mapstructure:"i"`
	// exchange code where the trade occurred
	Exchange string `mapstructure:"x"`
	// trade price
	Price float64 `mapstructure:"p"`
	// trade size (shares)
	Size int `mapstructure:"s"`
	// timestamp
	Timestamp time.Time `mapstructure:"t"`
	// condition flags
	Conditions []string `mapstructure:"c"`
	// tape
	Tape string `mapstructure:"z"`
}

// Quote is a quote
type Quote struct {
	// event name, always “q”
	// eventType string `mapstructure:"T"`
	// symbol
	Symbol string `mapstructure:"S"`
	// exchange code for bid quote
	BidExchange string `mapstructure:"bx"`
	// bid price
	BidPrice float64 `mapstructure:"bp"`
	// bid size
	BidSize int `mapstructure:"bs"`
	// exchange code for ask quote
	AskExchange string `mapstructure:"ax"`
	// ask price
	AskPrice float64 `mapstructure:"ap"`
	// ask size
	AskSize int `mapstructure:"as"`
	// condition flags
	Conditions []string `mapstructure:"c"`
	// tape
	Tape string `mapstructure:"z"`
	// timestamp
	Timestamp time.Time `mapstructure:"t"`
}

// MinuteAggregate is a minute aggregate
type MinuteAggregate struct {
	// event name, always “b”
	// eventType string `mapstructure:"T"`
	// symbol
	Symbol string `mapstructure:"S"`
	// volume
	Volume uint64 `mapstructure:"v"`
	// open price of the bar
	Open float64 `mapstructure:"o"`
	// close price of the bar
	Close float64 `mapstructure:"c"`
	// high price of the bar
	High float64 `mapstructure:"h"`
	// low price of the bar
	Low float64 `mapstructure:"l"`
	// time at the beginning of the window
	Timestamp time.Time `mapstructure:"t"`
}
