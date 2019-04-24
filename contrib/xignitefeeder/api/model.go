package api

import (
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/configs"
	"strings"
	"time"
)

// ---- Get Quotes endpoint ----
type GetQuotesResponse struct {
	DelaySec float32 `json:"Delay"`
	ArrayOfEquityQuote []EquityQuote `json:"ArrayOfEquityQuote"`
}

type EquityQuote struct {
	Outcome  string   `json:"Outcome"`
	Security Security `json:"Security"`
	Quote    Quote    `json:"Quote"`
}

type Security struct {
	Symbol string `json:"Symbol"`
}

type Quote struct {
	DateTime XigniteDateTime
	Ask      float32
	Bid      float32
}

type XigniteDateTime time.Time

// layout of Datetime string returned from Xignite API
const XigniteDateTimeLayout = "2006/01/02 15:04:05"

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

// ---- List Symbols endpoint ----
type ListSymbolsResponse struct {
	Outcome                    string                `json:"Outcome"`
	ArrayOfSecurityDescription []SecurityDescription `json:"ArrayOfSecurityDescription"`
}

type SecurityDescription struct {
	Symbol string `json:"Symbol"`
}

// ---- Get Quotes Range endpoint ----
type GetQuotesRangeResponse struct {
	Outcome              string          `json:"Outcome"`
	ArrayOfEndOfDayQuote []EndOfDayQuote `json:"ArrayOfEndOfDayQuote"`
}

type EndOfDayQuote struct {
	Date                  configs.CustomDay `json:"Date"`
	Open                  float32           `json:"Open"`
	High                  float32           `json:"High"`
	Low                   float32           `json:"Low"`
	Close                 float32           `json:"Close"`
	ExchangeOfficialClose float32           `json:"ExchangeOfficialClose"`
	Volume                float32           `json:"Volume"`
}
