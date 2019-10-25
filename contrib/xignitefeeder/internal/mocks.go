package internal

import (
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/io"
)

// ----------------

// MockSymbolsManager is a no-op SymbolsManager
type MockSymbolsManager struct {
	Identifiers      []string
	IndexIdentifiers []string
}

// GetAllIdentifiers returns the static identifiers
func (msm MockSymbolsManager) GetAllIdentifiers() []string {
	return msm.Identifiers
}

// GetAllIdentifiers returns the static index identifiers
func (msm MockSymbolsManager) GetAllIndexIdentifiers() []string {
	return msm.IndexIdentifiers
}

// ----------------

// MockAPIClient is a no-op API client
type MockAPIClient struct{}

// GetRealTimeQuotes returns an empty api response
func (mac *MockAPIClient) GetRealTimeQuotes(identifiers []string) (api.GetQuotesResponse, error) {
	return api.GetQuotesResponse{}, nil
}

// ListSymbols returns an empty api response
func (mac *MockAPIClient) ListSymbols(exchange string) (api.ListSymbolsResponse, error) {
	return api.ListSymbolsResponse{}, nil
}

// ListSymbols returns an empty api response
func (mac *MockAPIClient) ListIndexSymbols(indexGroup string) (api.ListIndexSymbolsResponse, error) {
	return api.ListIndexSymbolsResponse{}, nil
}

// GetQuotesRange returns an empty api response
func (mac *MockAPIClient) GetQuotesRange(i string, sd, ed time.Time) (resp api.GetQuotesRangeResponse, err error) {
	return api.GetQuotesRangeResponse{}, nil
}

// GetQuotesRange returns an empty api response
func (mac *MockAPIClient) GetIndexQuotesRange(i string, sd, ed time.Time,
) (resp api.GetIndexQuotesRangeResponse, err error) {
	return api.GetIndexQuotesRangeResponse{}, nil
}

// ----------------

// MockTimeChecker always returns Open
type MockTimeChecker struct{}

// IsOpen always returns Open
func (m *MockTimeChecker) IsOpen(t time.Time) bool {
	return true
}

// ----------------

// MockQuotesWriter is a no-op QuotesWriter
type MockQuotesWriter struct {
	WriteCount int
}

// Write increments the counter so that a unit test could assert how many times this function is called
func (m *MockQuotesWriter) Write(resp api.GetQuotesResponse) error {
	m.WriteCount++
	return nil
}

// ----------------

// MockMarketStoreWriter is a no-op MarketStoreWriter.
type MockMarketStoreWriter struct {
	WrittenCSM io.ColumnSeriesMap
}

// Write stores the argument to the struct and does nothing else.
func (m *MockMarketStoreWriter) Write(csm io.ColumnSeriesMap) error {
	m.WrittenCSM = csm
	return nil
}
