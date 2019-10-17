package api

import (
	"strings"
	"time"
)

// GetQuotesResponse is a response model for Get Quotes endpoint
type GetQuotesResponse struct {
	DelaySec           float32       `json:"Delay"`
	ArrayOfEquityQuote []EquityQuote `json:"ArrayOfEquityQuote"`
}

// EquityQuote object in GetQuotesResponse
type EquityQuote struct {
	Outcome  string    `json:"Outcome"`
	Message  string    `json:"Message"`
	Security *Security `json:"Security"`
	Quote    *Quote    `json:"Quote"`
}

// Security object in EquityQuote object
type Security struct {
	Symbol string `json:"Symbol"`
}

// Quote object in Equity Quote object
type Quote struct {
	DateTime    XigniteDateTime `json:"DateTime,omitempty"`
	Ask         float32         `json:"Ask"`
	AskDateTime XigniteDateTime `json:"AskDateTime,omitempty"`
	Bid         float32         `json:"Bid"`
	BidDateTime XigniteDateTime `json:"BidDateTime,omitempty"`
	// price of the most recent deal
	Last                           float32 `json:"Last"`
	Open                           float32 `json:"Open"`
	High                           float32 `json:"High"`
	Low                            float32 `json:"Low"`
	Close                          float32 `json:"Close"`
	PreviousClose                  float32 `json:"PreviousClose"`
	Volume                         int64   `json:"Volume"`
	ExchangeOfficialClose          float32 `json:"ExchangeOfficialClose"`
	PreviousExchangeOfficialClose  float32 `json:"PreviousExchangeOfficialClose"`
	ChangeFromPreviousClose        float32 `json:"ChangeFromPreviousClose"`
	PercentChangeFromPreviousClose float32 `json:"PercentChangeFromPreviousClose"`
	UTCOffSet                      int     `json:"UTCOffSet"`
}

// XigniteDateTime is a date time in XigniteDateTimeLayout format
type XigniteDateTime time.Time

// XigniteDateTimeLayout is a layout of Datetime string returned from Xignite API
const XigniteDateTimeLayout = "2006/01/02 15:04:05"

// UnmarshalJSON parses a string in the XigniteDateTime Layout
func (cd *XigniteDateTime) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "" {
		*cd = XigniteDateTime{}
		return nil
	}

	t, err := time.Parse(XigniteDateTimeLayout, s)
	if err != nil {
		return err
	}
	*cd = XigniteDateTime(t)

	return nil
}

// --------------------------

// XigniteDay is a date (yyyy/mm/dd) in XigniteDateTimeLayout format
type XigniteDay time.Time

// XigniteDay is a layout of Datetime string returned from Xignite GetQuotesRange API
const XigniteDayLayout = "2006/01/02"

// UnmarshalJSON parses a string in the XigniteDay Layout
func (cd *XigniteDay) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "" {
		*cd = XigniteDay{}
		return nil
	}

	t, err := time.Parse(XigniteDayLayout, s)
	if err != nil {
		return err
	}
	*cd = XigniteDay(t)

	return nil
}

// --------------------------

// ListSymbolsResponse is a response model for the List Symbols endpoint
type ListSymbolsResponse struct {
	Outcome                    string                `json:"Outcome"`
	Message                    string                `json:"Message"`
	ArrayOfSecurityDescription []SecurityDescription `json:"ArrayOfSecurityDescription"`
}

// SecurityDescription object in ListSymbolsResponse
type SecurityDescription struct {
	Symbol string `json:"Symbol"`
}

// --------------------------

// GetQuotesRangeResponse is a response model for the Get Quotes Range endpoint
type GetQuotesRangeResponse struct {
	Outcome              string          `json:"Outcome"`
	Message              string          `json:"Message"`
	Security             *Security       `json:"Security"`
	ArrayOfEndOfDayQuote []EndOfDayQuote `json:"ArrayOfEndOfDayQuote"`
}

// EndOfDayQuote object in GetQuotesRangeResponse
type EndOfDayQuote struct {
	Date                           XigniteDay `json:"Date"`
	Open                           float32    `json:"Open"`
	High                           float32    `json:"High"`
	Low                            float32    `json:"Low"`
	Close                          float32    `json:"Close"`
	ExchangeOfficialClose          float32    `json:"ExchangeOfficialClose"`
	PreviousClose                  float32    `json:"PreviousClose"`
	Volume                         int64      `json:"Volume"`
	PreviousExchangeOfficialClose  float32    `json:"PreviousExchangeOfficialClose"`
	ChangeFromPreviousClose        float32    `json:"ChangeFromPreviousClose"`
	PercentChangeFromPreviousClose float32    `json:"PercentChangeFromPreviousClose"`
}
