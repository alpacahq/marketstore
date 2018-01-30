package frontend

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
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

func Heartbeat(address string) {
	http.HandleFunc("/heartbeat", handler)
	http.ListenAndServe(address, nil)
}

func handler(rw http.ResponseWriter, r *http.Request) {
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
			Log(ERROR, "Failed to write heartbeat message - Error: %v", err)
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
			Log(ERROR, "Failed to write heartbeat message - Error: %v", err)
		}
	}
}
