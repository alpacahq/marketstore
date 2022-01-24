package symbols

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Manager manages symbols in the target stock exchanges.
// symbol(s) can be newly registered / removed from the exchange,
// so target symbols should be update periodically.
type Manager interface {
	GetAllIdentifiers() []string
	GetAllIndexIdentifiers() []string
}

// ManagerImpl is an implementation of the Manager.
type ManagerImpl struct {
	APIClient         api.Client
	TargetExchanges   []string
	TargetIndexGroups []string
	// identifier = {symbol}.{exchange} (i.e. "7203.XTKS").
	Identifiers map[string][]string
	// IndexIdentifiers are the identifiers for index symbols (ex. "151.INDXJPX" (=TOPIX))
	IndexIdentifiers map[string][]string
}

// NewManager initializes the SymbolManager object with the specified parameters.
func NewManager(apiClient api.Client, targetExchanges, targetIndexGroups []string) *ManagerImpl {
	return &ManagerImpl{
		APIClient: apiClient, TargetExchanges: targetExchanges, TargetIndexGroups: targetIndexGroups,
		Identifiers: map[string][]string{}, IndexIdentifiers: map[string][]string{},
	}
}

// GetAllIdentifiers returns Identifiers for the target symbols for all the target exchanges
// identifier = {exchange}.{symbol} (ex. "XTKS.1301").
func (m ManagerImpl) GetAllIdentifiers() []string {
	var identifiers []string
	for _, exchange := range m.TargetExchanges {
		identifiers = append(identifiers, m.Identifiers[exchange]...)
	}
	return identifiers
}

// GetAllIndexIdentifiers returns Identifiers for the target index symbols for all the index groups
// identifier = {exchange}.{symbol} (ex. "XTKS.1301").
func (m ManagerImpl) GetAllIndexIdentifiers() []string {
	var identifiers []string
	for _, exchange := range m.TargetIndexGroups {
		identifiers = append(identifiers, m.IndexIdentifiers[exchange]...)
	}
	return identifiers
}

// Update calls UpdateSymbols and UpdateIndexSymbols sequentially.
func (m ManagerImpl) Update() {
	m.UpdateSymbols()
	m.UpdateIndexSymbols()
}

// UpdateSymbols calls the ListSymbols endpoint, convert the symbols to the Identifiers
// and store them to the Identifiers map.
func (m ManagerImpl) UpdateSymbols() {
	for _, exchange := range m.TargetExchanges {
		resp, err := m.APIClient.ListSymbols(exchange)

		// if ListSymbols API returns an error, don't update the target symbols
		if err != nil || resp.Outcome != "Success" {
			log.Error(fmt.Sprintf("err=%v, List Symbols API response=%v", err, resp))
			return
		}

		// convert the symbol strings (i.e. "1234") to the identifier strings (i.e. "1234.XTKS") and store them to the map
		var identifiers []string
		for _, securityDescription := range resp.ArrayOfSecurityDescription {
			if securityDescription.Symbol != "" {
				identifier := fmt.Sprintf("%s.%s", securityDescription.Symbol, exchange)
				identifiers = append(identifiers, identifier)
			}
		}

		// update target symbols for the stock exchange
		m.Identifiers[exchange] = identifiers
		log.Debug(fmt.Sprintf("Updated symbols. The number of symbols in %s is %d", exchange, len(m.Identifiers[exchange])))
	}
}

// UpdateIndexSymbols calls the ListIndexSymbols endpoint, convert the index symbols to the Identifiers
// and store them to the Identifiers map.
func (m ManagerImpl) UpdateIndexSymbols() {
	for _, indexGroup := range m.TargetIndexGroups {
		resp, err := m.APIClient.ListIndexSymbols(indexGroup)

		// if ListSymbols API returns an error, don't update the target symbols
		if err != nil || resp.Outcome != "Success" {
			log.Error("UpdateIndexSymbols err=%v, API response=%v", err, resp)
			return
		}

		// convert the symbol strings (i.e. "1234") to the identifier strings (i.e. "1234.XTKS") and store them to the map
		var identifiers []string
		for _, index := range resp.ArrayOfIndex {
			if index.Symbol != "" {
				identifier := fmt.Sprintf("%s.%s", index.Symbol, indexGroup)
				identifiers = append(identifiers, identifier)
			}
		}

		// update target index symbols for the index group
		m.IndexIdentifiers[indexGroup] = identifiers
		log.Debug(fmt.Sprintf("Updated index symbols. The number of index symbols in %s is %d",
			indexGroup, len(m.IndexIdentifiers[indexGroup])))
	}
}
