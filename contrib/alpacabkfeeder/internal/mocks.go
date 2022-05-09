package internal

import (
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	v1 "github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api/v1"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// MockSymbolsManager is a no-op SymbolsManager.
type MockSymbolsManager struct {
	Symbols []string
}

// GetAllSymbols returns the static symbols.
func (msm MockSymbolsManager) GetAllSymbols() []string {
	return msm.Symbols
}

// UpdateSymbols does nothing.
func (msm MockSymbolsManager) UpdateSymbols() {}

// MockAPIClient is a no-op API client.
type MockAPIClient struct{}

// GetSnapshots returns an empty response.
func (mac *MockAPIClient) GetSnapshots(symbols []string) (map[string]*api.Snapshot, error) {
	return map[string]*api.Snapshot{}, nil
}

// ListAssets returns an empty api response.
func (mac *MockAPIClient) ListAssets(status *string) ([]v1.Asset, error) {
	return []v1.Asset{}, nil
}

// GetMultiBars returns an empty api response.
func (mac *MockAPIClient) GetMultiBars(symbols []string, opts api.GetBarsParams) (map[string][]api.Bar, error) {
	return map[string][]api.Bar{}, nil
}

// MockTimeChecker always returns Open.
type MockTimeChecker struct{}

// IsOpen always returns Open.
func (m *MockTimeChecker) IsOpen(t time.Time) bool {
	return true
}

// Sub always returns a date provided at the first argument.
func (m *MockTimeChecker) Sub(dateInJST time.Time, businessDay int) (time.Time, error) {
	return dateInJST, nil
}

// MockMarketStoreWriter is a no-op MarketStoreWriter.
type MockMarketStoreWriter struct {
	WrittenCSM io.ColumnSeriesMap
	Err        error
}

// Write stores the argument to the struct and does nothing else.
func (m *MockMarketStoreWriter) Write(csm io.ColumnSeriesMap) error {
	if m.Err != nil {
		return m.Err
	}

	m.WrittenCSM = csm
	return nil
}
