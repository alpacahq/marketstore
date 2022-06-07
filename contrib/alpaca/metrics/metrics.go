package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "alpaca"
	subsystem = "marketstore"
)

var (
	// AlpacaStreamLastUpdate stores the Unix time when the given (minute_bar, quote, trade) stream is updated.
	AlpacaStreamLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpaca_last_update_time",
			Help:      "Last update time of Alpaca streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaStreamUpdateLag stores the current lag in seconds
	// partitioned by type (minute_bar, quote, trade).
	AlpacaStreamUpdateLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpaca_update_lag",
			Help:      "Update lag of Alpaca streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaStreamMessagesHandled stores the number of
	// stream messages handled partitioned by type (minute_bar, quote, trade).
	AlpacaStreamMessagesHandled = promauto.NewCounterVec(
		// nolint: promlinter // TODO: counter metrics should have "_total" suffix
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpaca_messages_handled",
			Help:      "Number of stream messages handled, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaStreamQueueLength stores the number of
	// unprocessed messages currently in the queue.
	AlpacaStreamQueueLength = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpaca_waiting_queue_length",
			Help:      "Number of stream messages waiting for processing",
		},
	)
)
