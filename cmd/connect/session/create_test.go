package session

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/alpacahq/marketstore/v4/cmd/connect/session/mock"
	"github.com/alpacahq/marketstore/v4/frontend"
)

const exampleCommandFixed = `\create TEST/1Min/OHLCV Open,High,Low,Close/float32:Volume/int64 fixed`
const exampleCommandVariable = `\create TEST/1Sec/Tick Bid,Ask/float32 variable`

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
		{name: "error/Create API error",
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
