package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var namespace = "alpaca"
var subsystem = "marketstore"

var (
	// StartupTime stores how long the startup took (in seconds)
	StartupTime = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "startup_seconds",
			Help:      "Seconds taken by the startup",
		},
	)

	// RPCTotalRequestDuration stores the processing time for every request
	RPCTotalRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_total_request_duration_seconds",
		Help:      "RPC request processing time for every request",
	})

	// RPCTotalRequestsTotal stores the number of requests
	RPCTotalRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_total_requests_total",
		Help:      "Number of RPC requests received including ones resulting in errors",
	})

	// RPCSuccessfulRequestDuration stores the processing time for successful
	// requests partitioned by method
	RPCSuccessfulRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_successful_request_duration_seconds",
		Help:      "RPC request processing time for successful requests partitioned by method",
	}, []string{"method"})

	// RPCSuccessfulRequestsTotal stores the number of successful
	// requests partitioned by method
	RPCSuccessfulRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_successful_requests_total",
		Help:      "Number of RPC successful requests partitioned by method",
	}, []string{"method"})
)
