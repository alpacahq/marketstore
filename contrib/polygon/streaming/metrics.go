package streaming

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// evError stores the total number of stream errors, partitioned by error.
	evError = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "polygon",
			Subsystem: "stream",
			Name:      "error_total",
			Help:      "Total number of stream errors, partitioned by error.",
		},
		[]string{
			"error",
		},
	)

	// evUpdate stores the total number of stream updates Polygon sent, partitioned by ev.
	evUpdate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "polygon",
			Subsystem: "stream",
			Name:      "update_total",
			Help:      "Total number of stream updates, partitioned by ev.",
		},
		[]string{
			"ev",
		},
	)

	// evUpdateTime stores the last time in seconds Polygon sent an update on streaming API, partitioned by ev.
	evUpdateTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "polygon",
			Subsystem: "stream",
			Name:      "update_time",
			Help:      "Time in seconds Polygon sent an update, partitioned by ev.",
		},
		[]string{
			"ev",
		},
	)
)

func init() {
	prometheus.MustRegister(evError)
	prometheus.MustRegister(evUpdate)
	prometheus.MustRegister(evUpdateTime)
}
