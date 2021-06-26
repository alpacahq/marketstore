package sqlparser

import (
	"github.com/alpacahq/marketstore/v4/contrib/candler/candlecandler"
	"github.com/alpacahq/marketstore/v4/contrib/candler/tickcandler"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
	"github.com/alpacahq/marketstore/v4/uda/avg"
	"github.com/alpacahq/marketstore/v4/uda/count"
	"github.com/alpacahq/marketstore/v4/uda/gap"
	"github.com/alpacahq/marketstore/v4/uda/max"
	"github.com/alpacahq/marketstore/v4/uda/min"
)

var AggRegistry = map[string]uda.AggInterface{
	"tickcandler":   &tickcandler.TickCandler{},
	"candlecandler": &candlecandler.CandleCandler{},
	"count":         &count.Count{},
	"min":           &min.Min{},
	"max":           &max.Max{},
	"avg":           &avg.Avg{},
	"gap":           &gap.Gap{},
	"adjust":        &adjust.Adjust{},
}
