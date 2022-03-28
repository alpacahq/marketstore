package symbols

import (
	"reflect"
	"testing"

	v1 "github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api/v1"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/configs"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
)

type MockListAssetsAPIClient struct {
	internal.MockAPIClient
}

func (mac *MockListAssetsAPIClient) ListAssets(_ *string) ([]v1.Asset, error) {
	return []v1.Asset{
		{
			Name:     "Hello",
			Exchange: "BATS",
			Symbol:   "ABCD",
		},
		{
			Name:     "World",
			Exchange: "NASDAQ",
			Symbol:   "EFGH",
		},
		{
			Name:     "FOO",
			Exchange: "NASDAQ",
			Symbol:   "IJKL",
		},
		{
			Name:     "BAR",
			Exchange: "NYSE",
			Symbol:   "MNOP",
		},
	}, nil
}

func TestManagerImpl_UpdateSymbols(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := NewManager(&MockListAssetsAPIClient{}, []configs.Exchange{"NASDAQ", "NYSE"})

	// --- when ---
	SUT.UpdateSymbols()

	// --- then ---
	expectedSymbols := []string{"EFGH", "IJKL", "MNOP"}

	if !reflect.DeepEqual(
		SUT.Symbols,
		expectedSymbols,
	) {
		t.Errorf("symbols: want=%v, got=%v", expectedSymbols, SUT.Symbols)
	}
}
