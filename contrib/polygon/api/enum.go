package api

import "bytes"

type Prefix string

const (
	OfficialOpeningPrice Prefix = "OO."
	OfficialClosingPrice Prefix = "OC."
	OpeningPrice         Prefix = "O."
	ReOpeningPrice       Prefix = "RO."
	ClosingPrice         Prefix = "C."
	Trade                Prefix = "T."
	Quote                Prefix = "Q."
	Agg                  Prefix = "AM."
)

type SubscriptionScope struct {
	scope   string
	symbols []string
}

func NewSubscriptionScope(scope Prefix, symbols []string) *SubscriptionScope {
	if len(symbols) == 0 {
		symbols = append(symbols, "*")
	}
	return &SubscriptionScope{
		scope:   string(scope),
		symbols: symbols,
	}
}

func (s SubscriptionScope) getSubScope() string {
	var buf bytes.Buffer
	for i, sym := range s.symbols {
		buf.WriteString(s.scope + sym)
		if i < len(s.symbols)-1 {
			buf.WriteString(",")
		}
	}
	return buf.String()
}
