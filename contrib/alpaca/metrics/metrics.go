package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// AlpacaStreamLastUpdate stores the Unix time when the given (bar, quote, trade) stream is updated
	AlpacaStreamLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "alpaca",
			Subsystem: "marketstore",
			Name:      "alpaca_last_update_time",
			Help:      "Last update time of Alpaca streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaStreamUpdateLag stores the current lag in seconds
	// partitioned by type (bar, quote, trade)
	AlpacaStreamUpdateLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "alpaca",
			Subsystem: "marketstore",
			Name:      "alpaca_update_lag",
			Help:      "Update lag of Alpaca streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)
)
