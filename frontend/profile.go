package frontend

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

func Profile(address string) {
	r := http.NewServeMux()

	// Register pprof handlers
	r.HandleFunc("/pprof/", pprof.Index)
	r.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/pprof/profile", pprof.Profile)
	r.HandleFunc("/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/pprof/trace", pprof.Trace)

	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Error(fmt.Sprintf("listen and serve pprof endpoints. err=%v", err.Error()))
	}
}
