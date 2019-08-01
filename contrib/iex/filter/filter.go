// Package filter serves as a utility for filtering IEX symbols
package filter

// SymbolFilter defines a function type for filtering symbols
type SymbolFilter func(string) bool

var Filters = map[string]SymbolFilter{
	"SPY": SPY,
}
