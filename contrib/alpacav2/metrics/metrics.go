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
	// AlpacaV2StreamLastUpdate stores the Unix time when the given (minute_bar, quote, trade) stream is updated
	AlpacaV2StreamLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpacav2_last_update_time",
			Help:      "Last update time of AlpacaV2 streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaV2StreamUpdateLag stores the current lag in seconds
	// partitioned by type (minute_bar, quote, trade)
	AlpacaV2StreamUpdateLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpacav2_update_lag",
			Help:      "Update lag of Alpaca streams, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaV2StreamMessagesHandled stores the number of
	// stream messages handled partitioned by type (minute_bar, quote, trade)
	AlpacaV2StreamMessagesHandled = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpacav2_messages_handled",
			Help:      "Number of stream messages handled, partitioned by type",
		},
		[]string{
			"type",
		},
	)

	// AlpacaV2StreamQueueLength stores the number of
	// unprocessed messages currently in the queue
	AlpacaV2StreamQueueLength = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "alpacav2_waiting_queue_length",
			Help:      "Number of stream messages waiting for processing",
		},
	)
)
