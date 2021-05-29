package symbols

import (
	"reflect"
	"testing"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/internal"
)

type MockListSymbolsAPIClient struct {
	internal.MockAPIClient
}

func (mac *MockListSymbolsAPIClient) ListSymbols(exchange string) (api.ListSymbolsResponse, error) {
	if exchange == "XTKS" {
		return api.ListSymbolsResponse{
			Outcome: "Success",
			Message: "Mock response",
			ArrayOfSecurityDescription: []api.SecurityDescription{
				{Symbol: "1234"},
				{Symbol: "5678"},
			},
		}, nil
	}

	if exchange == "XJAS" {
		return api.ListSymbolsResponse{
			Outcome: "Success",
			Message: "Mock response",
			ArrayOfSecurityDescription: []api.SecurityDescription{
				{Symbol: "9012"},
			},
		}, nil
	}
	return api.ListSymbolsResponse{}, nil
}

func TestManagerImpl_UpdateSymbols(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := ManagerImpl{
		APIClient:       &MockListSymbolsAPIClient{},
		TargetExchanges: []string{"XTKS", "XJAS"},
		Identifiers:     map[string][]string{},
	}

	// --- when ---
	SUT.UpdateSymbols()

	// --- then ---
	expectedIdentifiers := map[string][]string{
		"XTKS": {"1234.XTKS", "5678.XTKS"},
		"XJAS": {"9012.XJAS"},
	}

	if !reflect.DeepEqual(
		SUT.Identifiers,
		expectedIdentifiers,
	) {
		t.Errorf("Identifier: want=%v, got=%v", expectedIdentifiers, SUT.Identifiers)
	}

}
