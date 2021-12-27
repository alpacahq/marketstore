package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"
)

const (
	DummyXigniteToken = "DUMMY"
)

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewTestResponseBody(t *testing.T, responseBodyModel interface{}) []byte {
	t.Helper()

	b, err := json.Marshal(responseBodyModel)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

func NewMockClient(t *testing.T, expectedResponse interface{}) *http.Client {
	t.Helper()

	// return
	returnNormal := func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewBuffer(NewTestResponseBody(t, expectedResponse))),
			Header:     make(http.Header),
		}
	}

	return &http.Client{
		Transport: RoundTripFunc(returnNormal),
	}
}

func TestDefaultClient_GetRealTimeQuotes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		httpClient   *http.Client
		identifiers  []string
		wantResponse GetQuotesResponse
		wantErr      bool
	}{
		{
			name:         "Success",
			httpClient:   NewMockClient(t, GetQuotesResponse{ArrayOfEquityQuote: []EquityQuote{{Outcome: "Success"}}}),
			identifiers:  []string{"foo"},
			wantResponse: GetQuotesResponse{ArrayOfEquityQuote: []EquityQuote{{Outcome: "Success"}}},
			wantErr:      false,
		},
		{
			name: "SystemError",
			httpClient: NewMockClient(t, GetQuotesResponse{
				ArrayOfEquityQuote: []EquityQuote{
					{
						Outcome: "SystemError",
						Message: "An unexpected error occurred.",
					},
				},
			}),
			identifiers: []string{"foo"},
			wantResponse: GetQuotesResponse{ArrayOfEquityQuote: []EquityQuote{
				{Outcome: "SystemError", Message: "An unexpected error occurred."},
			}},
			wantErr: false,
		},
		{
			name: "3 identifiers are requested but only 2 equity quotes are returned",
			httpClient: NewMockClient(t, GetQuotesResponse{
				ArrayOfEquityQuote: []EquityQuote{
					{Outcome: "Success", Message: "Success1"},
					{Outcome: "SystemError", Message: "An unexpected error occurred."},
				},
			}),
			identifiers: []string{"foo", "bar", "fizz"},
			wantResponse: GetQuotesResponse{ArrayOfEquityQuote: []EquityQuote{
				{Outcome: "Success", Message: "Success1"},
				{Outcome: "SystemError", Message: "An unexpected error occurred."},
			}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &DefaultClient{
				httpClient: tt.httpClient,
				token:      DummyXigniteToken,
			}
			gotResponse, err := c.GetRealTimeQuotes(tt.identifiers)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRealTimeQuotes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResponse, tt.wantResponse) {
				t.Errorf("GetRealTimeQuotes() gotResponse = %v, want %v", gotResponse, tt.wantResponse)
			}
		})
	}
}

func TestDefaultAPIClient_ListSymbols_Success(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: Success" response body
		httpClient: NewMockClient(t, ListSymbolsResponse{Outcome: "Success"}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	got, err := SUT.ListSymbols("foobar")
	// --- then ---
	if err != nil {
		t.Errorf("Error should be nil. Err = %v", err)
	}
	if got.Outcome != "Success" {
		t.Errorf("Outcome = %v, want %v", got.Outcome, "Success")
	}
}

func TestDefaultAPIClient_GetQuotesRange_Success(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: Success" response body
		httpClient: NewMockClient(t, GetQuotesRangeResponse{Outcome: "Success"}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	got, err := SUT.GetQuotesRange("foobar", time.Time{}, time.Time{})
	// --- then ---
	if err != nil {
		t.Errorf("Error should be nil. Err = %v", err)
	}
	if got.Outcome != "Success" {
		t.Errorf("Outcome = %v, want %v", got.Outcome, "Success")
	}
}

// When Xignite returns Outcome:"SystemError", throw an error.
func TestDefaultAPIClient_ListSymbols_Error(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: SystemError" response body
		httpClient: NewMockClient(t, ListSymbolsResponse{Outcome: "SystemError"}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	_, err := SUT.ListSymbols("foobar")

	// --- then ---
	if err == nil {
		t.Errorf("An error should be returned when the Outcome is not 'Success' s")
	}
}

// When Xignite returns Outcome:"SystemError" to ListIndexSymbols API, throw an error.
func TestDefaultAPIClient_ListIndexSymbols_Error(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: SystemError" response body
		httpClient: NewMockClient(t, ListIndexSymbolsResponse{Outcome: "SystemError"}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	_, err := SUT.ListIndexSymbols("exampleIndexGroup")

	// --- then ---
	if err == nil {
		t.Errorf("An error should be returned when the Outcome is not 'Success' s")
	}
}

func TestDefaultAPIClient_GetRealTimeBars_Success(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: Success" response body
		httpClient: NewMockClient(t, GetBarsResponse{Outcome: "Success", ArrayOfBar: []Bar{}}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	got, err := SUT.GetRealTimeBars("foobar", time.Now(), time.Now())
	// --- then ---
	if err != nil {
		t.Fatalf("Error should be nil. Err = %v", err)
	}
	if got.Outcome != "Success" {
		t.Errorf("Outcome = %v, want %v", got.Outcome, "Success")
	}
}

func TestDefaultAPIClient_GetIndexBars_Success(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: Success" response body
		httpClient: NewMockClient(t, GetIndexBarsResponse{Outcome: "Success", ArrayOfBar: []Bar{}}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	got, err := SUT.GetIndexBars("foobar", time.Now(), time.Now())
	// --- then ---
	if err != nil {
		t.Fatalf("Error should be nil. Err = %v", err)
	}
	if got.Outcome != "Success" {
		t.Errorf("Outcome = %v, want %v", got.Outcome, "Success")
	}
}

// When Xignite returns Outcome:"SystemError", throw an error.
func TestDefaultAPIClient_GetQuotesRange_Error(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultClient{
		// return "Outcome: SystemError" response body
		httpClient: NewMockClient(t, GetQuotesRangeResponse{Outcome: "SystemError"}),
		token:      DummyXigniteToken,
	}

	// --- when ---
	_, err := SUT.GetQuotesRange("foobar", time.Time{}, time.Time{})

	// --- then ---
	if err == nil {
		t.Errorf("An error should be returned when the Outcome is not 'Success'.")
	}
}
