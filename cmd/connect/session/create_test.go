package session

import (
	"errors"
	"testing"

	"github.com/alpacahq/marketstore/v4/frontend"
)

type mockRPCClient struct {
	rpcResp *frontend.MultiServerResponse
	rpcErr  error
}

func (m *mockRPCClient) DoRPC(_ string, _ interface{}) (response interface{}, err error) {
	return m.rpcResp, m.rpcErr
}

const exampleCommandFixed = `\create TEST/1Min/OHLCV Open,High,Low,Close/float32:Volume/int64 fixed`
const exampleCommandVariable = `\create TEST/1Sec/Tick Bid,Ask/float32 variable`

func TestClient_create(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		line    string
		rpcResp *frontend.MultiServerResponse
		rpcErr  error
		wantOk  bool
	}{
		{
			name:    "success/fixed record",
			line:    exampleCommandFixed,
			rpcResp: &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk:  true,
		},
		{
			name:    "success/variable-length record",
			line:    exampleCommandVariable,
			rpcResp: &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk:  true,
		},
		{
			name:    "error/invalid record type",
			line:    `\create TEST/1Sec/Tick Bid,Ask/float32 foobarfizzbuzz`,
			rpcResp: &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk:  false,
		},
		{
			name:   "error/not enough arguments",
			line:   `\create firstArg secondArg`,
			wantOk: false,
		},
		{
			name:   "error/invalid typestr",
			line:   `\create TEST/1Min/OCV Open,Close/float128:Volume/int64 fixed`, // float128 is invallid
			wantOk: false,
		},
		{
			name:   "error/rpc Error",
			line:   exampleCommandFixed,
			rpcErr: errors.New("error"),
			wantOk: false,
		},
		{name: "error/Create API error",
			line:    exampleCommandFixed,
			rpcResp: &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{{Error: "API errosr!"}}},
			rpcErr:  nil,
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		rc := mockRPCClient{rpcResp: tt.rpcResp, rpcErr: tt.rpcErr}

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &Client{
				mode: remote,
				rc:   &rc,
			}
			if gotOk := c.create(tt.line); gotOk != tt.wantOk {
				t.Errorf("create() = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
