package session

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/alpacahq/marketstore/v4/cmd/connect/session/mock"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

const (
	exampleCommandFixed    = `\create TEST/1Min/OHLCV Open,High,Low,Close/float32:Volume/int64 fixed`
	exampleCommandVariable = `\create TEST/1Sec/Tick Bid,Ask/float32 variable`
)

func TestClient_create(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		line   string
		resp   *frontend.MultiServerResponse
		err    error
		wantOk bool
	}{
		{
			name:   "success/fixed record",
			line:   exampleCommandFixed,
			resp:   &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk: true,
		},
		{
			name:   "success/variable-length record",
			line:   exampleCommandVariable,
			resp:   &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk: true,
		},
		{
			name:   "error/invalid record type",
			line:   `\create TEST/1Sec/Tick Bid,Ask/float32 foobarfizzbuzz`,
			resp:   &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{}},
			wantOk: false,
		},
		{
			name:   "error/not enough arguments",
			line:   `\create firstArg secondArg`,
			wantOk: false,
		},
		{
			name:   "error/invalid typestr",
			line:   `\create TEST/1Min/OCV Open,Close/float128:Volume/int64 fixed`, // float128 is invalid
			wantOk: false,
		},
		{
			name:   "error/rpc Error",
			line:   exampleCommandFixed,
			err:    errors.New("error"),
			wantOk: false,
		},
		{
			name:   "error/Create API error",
			line:   exampleCommandFixed,
			resp:   &frontend.MultiServerResponse{Responses: []frontend.ServerResponse{{Error: "API errors!"}}},
			err:    nil,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockCtrl := gomock.NewController(t)
			mockClient := mock.NewMockAPIClient(mockCtrl)
			mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
				func(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error {
					if responses != nil && tt.resp != nil {
						*responses = *tt.resp
					}
					return tt.err
				},
			)

			c := NewClient(mockClient)
			if gotOk := c.create(tt.line); gotOk != tt.wantOk {
				t.Errorf("create() = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestClient_GetBucketInfo(t *testing.T) {
	t.Parallel()
	var (
		testGetInfoResponse = frontend.GetInfoResponse{
			LatestYear: 2021,
			TimeFrame:  1 * time.Minute,
			DSV:        []io.DataShape{{Name: "Epoch", Type: io.INT64}, {Name: "Ask", Type: io.FLOAT32}},
			RecordType: io.VARIABLE,
			ServerResp: frontend.ServerResponse{},
		}

		testGetInfoErrResponse = frontend.GetInfoResponse{
			ServerResp: frontend.ServerResponse{Error: "error!"},
		}
	)

	tests := []struct {
		name     string
		key      *io.TimeBucketKey
		resp     *frontend.MultiGetInfoResponse
		err      error
		wantResp *frontend.GetInfoResponse
		wantErr  bool
	}{
		{
			name:     "success/bucket info returned",
			key:      io.NewTimeBucketKey("TEST/1Min/TICK"),
			resp:     &frontend.MultiGetInfoResponse{Responses: []frontend.GetInfoResponse{testGetInfoResponse}},
			wantResp: &testGetInfoResponse,
		},
		{
			name:     "error/no bucket info is returned",
			key:      io.NewTimeBucketKey("TEST/1Min/TICK"),
			resp:     &frontend.MultiGetInfoResponse{Responses: []frontend.GetInfoResponse{}}, // empty
			wantResp: nil,
			wantErr:  true,
		},
		{
			name:     "error/nil response is returned",
			key:      io.NewTimeBucketKey("TEST/1Min/TICK"),
			resp:     nil,
			wantResp: nil,
			wantErr:  true,
		},
		{
			name:     "error/error is passed",
			key:      io.NewTimeBucketKey("TEST/1Min/TICK"),
			err:      errors.New("error"),
			wantResp: nil,
			wantErr:  true,
		},
		{
			name:     "error/error in Server Response struct is passed",
			key:      io.NewTimeBucketKey("TEST/1Min/TICK"),
			resp:     &frontend.MultiGetInfoResponse{Responses: []frontend.GetInfoResponse{testGetInfoErrResponse}},
			wantResp: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			mockCtrl := gomock.NewController(t)
			mockClient := mock.NewMockAPIClient(mockCtrl)
			mockClient.EXPECT().GetBucketInfo(gomock.Any(), gomock.Any()).DoAndReturn(
				func(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse) error {
					if responses != nil && tt.resp != nil {
						*responses = *tt.resp
					}
					return tt.err
				},
			)

			c := &Client{apiClient: mockClient}

			// --- when ---
			gotResp, err := c.GetBucketInfo(tt.key)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBucketInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("GetBucketInfo() gotResp = %v, want %v", gotResp, tt.wantResp)
			}
		})
	}
}
