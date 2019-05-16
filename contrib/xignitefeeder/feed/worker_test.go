package feed

import (
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/tests"
	"testing"
)

func TestWorker_try_normal(t *testing.T) {
	// --- given ---
	w := &tests.MockQuotesWriter{WriteCount: 0}
	SUT := Worker{
		MarketTimeChecker: &tests.MockTimeChecker{},
		APIClient:         &tests.MockAPIClient{},
		SymbolManager:     tests.MockSymbolsManager{},
		QuotesWriter:      w,
		Interval:          1,
	}
	// --- when ---
	err := SUT.try()

	// --- then ---
	if w.WriteCount != 1 {
		t.Errorf("write should be performed once")
	}
	if err != nil {
		t.Errorf("error should be nil. err=%v", err)
	}
}
