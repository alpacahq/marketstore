package session

import (
	"github.com/alpacahq/marketstore/v4/frontend"
	"testing"
)

type mockRPCClient struct {
	rpcResp *frontend.MultiServerResponse
	rpcErr  error
}

func (m *mockRPCClient) DoRPC(_ string, _ interface{}) (response interface{}, err error) {
	return m.rpcResp, m.rpcErr
}

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
			name:    "success",
			line:    `\create TEST/1Min/OHLCV Open,High,Low,Close/float32:Volume/int64 fixed`,
			rpcResp: &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			rpcErr:  nil,
			wantOk:  true,
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
