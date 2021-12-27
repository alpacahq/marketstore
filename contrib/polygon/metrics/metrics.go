package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PolygonStreamLastUpdate stores the Unix time when the given (bar, quote, trade) stream is updated
var PolygonStreamLastUpdate = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "alpaca",
		Subsystem: "marketstore",
		Name:      "polygon_last_update_time",
		Help:      "Last update time of Polygon streams, partitioned by type",
	},
	[]string{
		"type",
	},
)
