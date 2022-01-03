package session_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/cmd/connect/session"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var exampleGetInfoResponse = frontend.GetInfoResponse{
	LatestYear: 2021,
	TimeFrame:  1 * time.Second,
	DSV:        []io.DataShape{},
	RecordType: io.FIXED,
}

type mockRpcClient struct {
	resp interface{}
	err  error
}

func (mc mockRpcClient) DoRPC(_ string, args interface{}) (response interface{}, err error) {
	return mc.resp, mc.err
}

func TestRemoteAPIClient_GetBucketInfo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		rpcClient     session.RPCClient
		reqs          *frontend.MultiKeyRequest
		wantResponses *frontend.MultiGetInfoResponse
		wantErr       bool
	}{
		{
			name: "Success",
			rpcClient: mockRpcClient{
				resp: &frontend.MultiGetInfoResponse{
					Responses: []frontend.GetInfoResponse{exampleGetInfoResponse},
				}, err: nil,
			},
			wantResponses: &frontend.MultiGetInfoResponse{
				Responses: []frontend.GetInfoResponse{exampleGetInfoResponse},
			},
			wantErr: false,
		},
		{
			name:          "error/unexpected interface returned from DoRPC",
			rpcClient:     mockRpcClient{resp: "aaaaa", err: nil},
			wantResponses: &frontend.MultiGetInfoResponse{},
			wantErr:       true,
		},
		{
			name:          "error/error returned from DoRPC",
			rpcClient:     mockRpcClient{resp: nil, err: errors.New("some error")},
			wantResponses: &frontend.MultiGetInfoResponse{},
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			rc := session.NewRemoteAPIClient("exampleurl:1234", tt.rpcClient)

			// --- when ---
			responses := &frontend.MultiGetInfoResponse{}
			err := rc.GetBucketInfo(tt.reqs, responses)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBucketInfo() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(responses, tt.wantResponses) {
				t.Errorf("GetBucketInfo() got = %v, want %v", responses, tt.wantResponses)
			}
		})
	}
}
