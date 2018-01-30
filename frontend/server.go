package frontend

import (
	"errors"
	"net/http"

	rpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"

	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/marketstore/utils/rpc/msgpack2"
)

var (
	queryableError = errors.New("Server is not queryable")
	argsNilError   = errors.New("Arguments are nil, can not query using nil arguments")
)

type DataService struct{}

func (s *DataService) Init() {}

type RpcServer struct {
	*rpc.Server
}

func (s *RpcServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("marketstore-version", utils.GitHash)
	s.Server.ServeHTTP(w, r)
}

func NewServer() (*RpcServer, *DataService) {
	s := &RpcServer{
		Server: rpc.NewServer(),
	}
	s.RegisterCodec(json2.NewCodec(), "application/json")
	s.RegisterCodec(json2.NewCodec(), "application/json;charset=UTF-8")
	s.RegisterCodec(msgpack2.NewCodec(), "application/x-msgpack")
	service := new(DataService)
	service.Init()
	err := s.RegisterService(service, "")
	if err != nil {
		Log(ERROR, "Failed to register service - Error: %v", err)
	}
	return s, service
}
