package symbols_test

import (
	"bytes"
	"io"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
)

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func TestJsonFileManager_UpdateSymbols(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mockResponse *http.Response
		wantSymbols  []string
	}{
		"OK/tradable stocks are retrieved from the json file": {
			mockResponse: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBuffer([]byte(mockTradableStocksJSON))),
				Header:     make(http.Header),
			},
			wantSymbols: []string{"AAPL", "ACN", "ADBE"},
		},
		"NG/json file is not found": {
			mockResponse: &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       nil,
				Header:     make(http.Header),
			},
			wantSymbols: []string{},
		},
		"NG/json file has an unexpected format": {
			mockResponse: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBuffer([]byte(unexpectedJSON))),
				Header:     make(http.Header),
			},
			wantSymbols: []string{},
		},
		"NG/unauthorized": {
			mockResponse: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       nil,
				Header:     make(http.Header),
			},
			wantSymbols: []string{},
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			httpClient := NewTestClient(func(req *http.Request) *http.Response { return tt.mockResponse })
			m := symbols.NewJSONFileManager(httpClient, "test", "user:pass")

			// --- when ---
			m.UpdateSymbols()

			// --- then ---
			require.Equal(t, sortStrSlice(tt.wantSymbols), sortStrSlice(m.GetAllSymbols()))
		})
	}
}

func sortStrSlice(s []string) []string {
	sort.SliceStable(s, func(i, j int) bool {
		return s[i] < s[j]
	})
	return s
}
