package internal

import (
	"context"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// MockSymbolsManager is a no-op SymbolsManager.
type MockSymbolsManager struct {
	Identifiers      []string
	IndexIdentifiers []string
}

// GetAllIdentifiers returns the static identifiers.
func (msm MockSymbolsManager) GetAllIdentifiers() []string {
	return msm.Identifiers
}

// GetAllIndexIdentifiers returns the static index identifiers.
func (msm MockSymbolsManager) GetAllIndexIdentifiers() []string {
	return msm.IndexIdentifiers
}

// MockAPIClient is a no-op API client.
type MockAPIClient struct{}

// GetRealTimeQuotes returns an empty api response.
func (mac *MockAPIClient) GetRealTimeQuotes(_ context.Context, _ []string) (api.GetQuotesResponse, error) {
	return api.GetQuotesResponse{}, nil
}

// ListSymbols returns an empty api response.
func (mac *MockAPIClient) ListSymbols(_ context.Context, _ string) (api.ListSymbolsResponse, error) {
	return api.ListSymbolsResponse{}, nil
}

// ListIndexSymbols returns an empty api response.
func (mac *MockAPIClient) ListIndexSymbols(_ context.Context, _ string) (api.ListIndexSymbolsResponse, error) {
	return api.ListIndexSymbolsResponse{}, nil
}

// GetRealTimeBars returns an empty api response.
func (mac *MockAPIClient) GetRealTimeBars(_ context.Context, _ string, _, _ time.Time,
) (response api.GetBarsResponse, err error) {
	return api.GetBarsResponse{
		Security:   &api.Security{Symbol: "123"},
		ArrayOfBar: []api.Bar{},
	}, nil
}

// GetIndexBars returns an empty api response.
func (mac *MockAPIClient) GetIndexBars(_ context.Context, _ string, _, _ time.Time,
) (response api.GetIndexBarsResponse, err error) {
	return api.GetIndexBarsResponse{}, nil
}

// GetQuotesRange returns an empty api response.
func (mac *MockAPIClient) GetQuotesRange(_ context.Context, _ string, _, _ time.Time,
) (resp api.GetQuotesRangeResponse, err error) {
	return api.GetQuotesRangeResponse{
		Security:             &api.Security{Symbol: "123"},
		ArrayOfEndOfDayQuote: []api.EndOfDayQuote{},
	}, nil
}

// GetIndexQuotesRange returns an empty api response.
func (mac *MockAPIClient) GetIndexQuotesRange(_ context.Context, _ string, _, _ time.Time,
) (resp api.GetIndexQuotesRangeResponse, err error) {
	return api.GetIndexQuotesRangeResponse{}, nil
}

// MockTimeChecker always returns Open.
type MockTimeChecker struct{}

// IsOpen always returns Open.
func (m *MockTimeChecker) IsOpen(_ time.Time) bool {
	return true
}

// Sub always returns a date provided at the first argument.
func (m *MockTimeChecker) Sub(dateInJST time.Time, _ int) (time.Time, error) {
	return dateInJST, nil
}

// MockQuotesWriter is a no-op QuotesWriter.
type MockQuotesWriter struct {
	WriteCount int
}

// Write increments the counter so that a unit test could assert how many times this function is called.
func (m *MockQuotesWriter) Write(_ api.GetQuotesResponse) error {
	m.WriteCount++
	return nil
}

// MockMarketStoreWriter is a no-op MarketStoreWriter.
type MockMarketStoreWriter struct {
	WrittenCSM io.ColumnSeriesMap
}

// Write stores the argument to the struct and does nothing else.
func (m *MockMarketStoreWriter) Write(csm io.ColumnSeriesMap) error {
	m.WrittenCSM = csm
	return nil
}
