package enums

type Prefix string

const (
	TradeEvent Prefix = "T"
	QuoteEvent Prefix = "Q"
	AggEvent   Prefix = "AM"
	sep        Prefix = "."
	Trade      Prefix = TradeEvent + sep
	Quote      Prefix = QuoteEvent + sep
	Agg        Prefix = AggEvent + sep
)
