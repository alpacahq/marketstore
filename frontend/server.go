package frontend

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/rpc/msgpack2"
	rpc "github.com/alpacahq/rpc/rpc2"
	"github.com/alpacahq/rpc/rpc2/json2"
)

var (
	queryableError = errors.New("Server is not queryable")
	argsNilError   = errors.New("Arguments are nil, can not query using nil arguments")
)

func NewDataService(disableVariableCompression, enableLastKnown bool, rootDir string) *DataService {
	return &DataService{
		disableVariableCompression: disableVariableCompression,
		enableLastKnown:            enableLastKnown,
		rootDir:                    rootDir,
	}
}

type DataService struct {
	disableVariableCompression bool
	enableLastKnown            bool
	rootDir                    string
}

func (s *DataService) Init() {}

type RpcServer struct {
	*rpc.Server
}

func (s *RpcServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("marketstore-version", utils.GitHash)
	s.Server.ServeHTTP(w, r)
	metrics.RPCTotalRequestDuration.Observe(time.Since(start).Seconds())
}

func NewServer(disableVariableCompression, enableLastKnown bool, rootDir string) (*RpcServer, *DataService) {
	s := &RpcServer{
		Server: rpc.NewServer(),
	}
	s.RegisterCodec(json2.NewCodec(), "application/json")
	s.RegisterCodec(json2.NewCodec(), "application/json;charset=UTF-8")
	s.RegisterCodec(msgpack2.NewCodec(), "application/x-msgpack")
	s.RegisterInterceptFunc(intercept)
	s.RegisterAfterFunc(after)
	service := NewDataService(disableVariableCompression, enableLastKnown, rootDir)
	service.Init()
	err := s.RegisterService(service, "")
	if err != nil {
		log.Error("Failed to register service - Error: %v", err)
	}
	return s, service
}

type key int

const startTimeKey key = 0

func intercept(i *rpc.RequestInfo) *http.Request {
	return i.Request.Clone(context.WithValue(i.Request.Context(), startTimeKey, time.Now()))
}

func after(i *rpc.RequestInfo) {
	v := i.Request.Context().Value(startTimeKey)
	if v == nil {
		log.Error("start time not set on context")
		return
	}
	t, ok := v.(time.Time)
	if !ok {
		log.Error("start time not correct type")
		return
	}

	metrics.RPCSuccessfulRequestDuration.WithLabelValues(i.Method).Observe(time.Since(t).Seconds())
}
