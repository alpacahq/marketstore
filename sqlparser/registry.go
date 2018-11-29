package sqlparser

import (
	"github.com/alpacahq/marketstore/contrib/candler/candlecandler"
	"github.com/alpacahq/marketstore/contrib/candler/tickcandler"
	"github.com/alpacahq/marketstore/uda"
	"github.com/alpacahq/marketstore/uda/avg"
	"github.com/alpacahq/marketstore/uda/count"
	"github.com/alpacahq/marketstore/uda/max"
	"github.com/alpacahq/marketstore/uda/min"
)

var AggRegistry = map[string]uda.AggInterface{
	"TickCandler":   &tickcandler.TickCandler{},
	"tickcandler":   &tickcandler.TickCandler{},
	"CandleCandler": &candlecandler.CandleCandler{},
	"candlecandler": &candlecandler.CandleCandler{},
	"Count":         &count.Count{},
	"count":         &count.Count{},
	"Min":           &min.Min{},
	"min":           &min.Min{},
	"Max":           &max.Max{},
	"max":           &max.Max{},
	"Avg":           &avg.Avg{},
	"avg":           &avg.Avg{},
}
