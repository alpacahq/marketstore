package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
)

const (
	XigniteToken = "DUMMY"
)

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewTestClient(t *testing.T) *http.Client {
	t.Helper()

	body := GetQuotesResponse{
		ArrayOfEquityQuote: []EquityQuote{{Outcome: "Success"}},
	}

	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	returnNormal := func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewBuffer(b)),
			Header:     make(http.Header),
		}
	}

	return &http.Client{
		Transport: RoundTripFunc(returnNormal),
	}
}

func TestDefaultAPIClient_GetRealTimeQuotes(t *testing.T) {
	tests := []struct {
		name        string
		client      Client
		identifiers []string
		want        GetQuotesResponse
		wantErr     bool
	}{
		// TODO: Add test cases.
		{
			name: "normal",
			client: &DefaultClient{
				httpClient: NewTestClient(t),
				token:      XigniteToken,
			},
			//NewDefaultAPIClient(XigniteToken, 30),
			identifiers: []string{"6501.XTKS"},
			want: GetQuotesResponse{ArrayOfEquityQuote:
			[]EquityQuote{{Outcome: "Success"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.client
			got, err := c.GetRealTimeQuotes(tt.identifiers)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultClient.GetRealTimeQuotes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultClient.GetRealTimeQuotes() = %v, want %v", got, tt.want)
			}
		})
	}
}
