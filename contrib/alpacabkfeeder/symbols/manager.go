package symbols

import (
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/alpaca"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var status = "active"

// Manager manages symbols in the target stock exchanges.
// symbol(s) can be newly registered / removed from the exchange,
// so target symbols should be updated periodically
type Manager interface {
	GetAllSymbols() []string
}

type APIClient interface {
	ListAssets(status *string) ([]alpaca.Asset, error)
}

// ManagerImpl is an implementation of the Manager.
type ManagerImpl struct {
	APIClient APIClient
	// Key: exchange(e.g. "NYSE")
	TargetExchanges map[string]struct{}
	Symbols         []string
}

// NewManager initializes the SymbolManager object with the specified parameters.
func NewManager(apiClient APIClient, targetExchanges []string) *ManagerImpl {
	exchanges := make(map[string]struct{}, 0)
	for _, exchange := range targetExchanges {
		exchanges[exchange] = struct{}{}
	}

	return &ManagerImpl{APIClient: apiClient, TargetExchanges: exchanges,
		Symbols: []string{}}
}

// GetAllSymbols returns Symbols for all the target exchanges
func (m *ManagerImpl) GetAllSymbols() []string {
	return m.Symbols
}

// UpdateSymbols calls the ListSymbols endpoint, convert the symbols to the Symbols and store them to the Symbols map
func (m *ManagerImpl) UpdateSymbols() {
	assets, err := m.APIClient.ListAssets(&status)

	// if ListAssets API returns an error, don't update the target symbols
	if err != nil {
		log.Error(fmt.Sprintf("ListAssets: err=%v, API response=%v", err, assets))
		return
	}

	// add symbols of exchanges in the target exchange list
	symbols := []string{}
	for _, asset := range assets {
		if _, found := m.TargetExchanges[asset.Exchange]; found {
			symbols = append(symbols, asset.Symbol)
		}
	}

	// replace target symbols
	m.Symbols = symbols
	log.Debug(fmt.Sprintf("Updated symbols. The number of symbols is %d", len(m.Symbols)))
}
