package enums

type Prefix string

const (
	TradeEvent       Prefix = "T"
	QuoteEvent       Prefix = "Q"
	AggToMinuteEvent Prefix = "AM"
	sep              Prefix = "."
	Trade            Prefix = TradeEvent + sep
	Quote            Prefix = QuoteEvent + sep
	AggToMinute      Prefix = AggToMinuteEvent + sep
)
