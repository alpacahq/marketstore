package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
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

func TestDefaultAPIClient_GetRealTimeQuotes_Success(t *testing.T) {
	// --- given ---
	SUT := &DefaultClient{
		httpClient: NewMockClient(t,
			// return "Outcome: Success" response body
			GetQuotesResponse{ArrayOfEquityQuote: []EquityQuote{{Outcome: "Success"}},},
		),
		token: DummyXigniteToken,}

	// --- when ---
	got, err := SUT.GetRealTimeQuotes([]string{"hoge"})

	// --- then ---
	if err != nil {
		t.Fatalf("Error should be nil. Err = %v", err)
	}
	if got.ArrayOfEquityQuote[0].Outcome != "Success" {
		t.Errorf("Outcome = %v, want %v", got.ArrayOfEquityQuote[0].Outcome, "Success")
	}
}

func TestDefaultAPIClient_ListSymbols_Success(t *testing.T) {
	// --- given ---
	SUT := &DefaultClient{
		httpClient: NewMockClient(t,
			// return "Outcome: Success" response body
			ListSymbolsResponse{Outcome: "Success"},
		),
		token: DummyXigniteToken,}

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
