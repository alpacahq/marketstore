package frontend

import (
	"net/http"
	"net/http/pprof"
)

func Profile(address string) {
	r := http.NewServeMux()

	// Register pprof handlers
	r.HandleFunc("/pprof/", pprof.Index)
	r.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/pprof/profile", pprof.Profile)
	r.HandleFunc("/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/pprof/trace", pprof.Trace)

	http.ListenAndServe(address, nil)
}
