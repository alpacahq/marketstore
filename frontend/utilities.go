package frontend

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var Queryable uint32 // treated as bool

type HeartbeatMessage struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	Uptime  string `json:"uptime"`
}

func init() {
	Queryable = uint32(0)
}

func Utilities(url string) {
	// heartbeat
	http.HandleFunc("/heartbeat", heartbeat)

	// profiling
	http.HandleFunc("/pprof/", pprof.Index)
	http.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	http.Handle("/pprof/heap", pprof.Handler("heap"))
	http.Handle("/pprof/goroutine", pprof.Handler("goroutine"))
	http.Handle("/pprof/threadcreate", pprof.Handler("threadcreate"))
	http.Handle("/pprof/block", pprof.Handler("block"))

	http.ListenAndServe(url, nil)
}

func heartbeat(rw http.ResponseWriter, r *http.Request) {
	uptime := time.Since(utils.InstanceConfig.StartTime).String()
	queryable := atomic.LoadUint32(&Queryable)
	if queryable > 0 {
		// queryable
		rw.WriteHeader(http.StatusOK)
		err := json.NewEncoder(rw).Encode(HeartbeatMessage{
			Status:  "queryable",
			Version: utils.Tag,
			GitHash: utils.GitHash,
			Uptime:  uptime,
		})
		if err != nil {
			log.Error("Failed to write heartbeat message - Error: %v", err)
		}
	} else {
		// not queryable
		rw.WriteHeader(http.StatusServiceUnavailable)
		err := json.NewEncoder(rw).Encode(HeartbeatMessage{
			Status:  "not queryable",
			Version: utils.Tag,
			GitHash: utils.GitHash,
			Uptime:  uptime,
		})
		if err != nil {
			log.Error("Failed to write heartbeat message - Error: %v", err)
		}
	}
}
