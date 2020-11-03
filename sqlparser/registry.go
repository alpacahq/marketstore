package sqlparser

import (
	"github.com/alpacahq/marketstore/v4/contrib/candler/candlecandler"
	"github.com/alpacahq/marketstore/v4/contrib/candler/tickcandler"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/uda/avg"
	"github.com/alpacahq/marketstore/v4/uda/count"
	"github.com/alpacahq/marketstore/v4/uda/gap"
	"github.com/alpacahq/marketstore/v4/uda/max"
	"github.com/alpacahq/marketstore/v4/uda/min"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
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
	"Gap":           &gap.Gap{},
	"gap":           &gap.Gap{},
	"Adjust":		 &adjust.Adjust{},
	"adjust":		 &adjust.Adjust{},
}
