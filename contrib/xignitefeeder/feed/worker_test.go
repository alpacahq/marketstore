package feed

import (
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/internal"
	"testing"
	"time"
)

// MockTimeChecker always returns Open
type MockTimeChecker struct{}

// IsOpen always returns Open
func (m *MockTimeChecker) IsOpen(t time.Time) bool {
	return true
}

// MockQuotesWriter is a no-op QuotesWriter
type MockQuotesWriter struct {
	WriteCount int
}

// Write increments the counter so that a unit test could assert how many times this function is called
func (m *MockQuotesWriter) Write(resp api.GetQuotesResponse) error {
	m.WriteCount++
	return nil
}

func TestWorker_try_normal(t *testing.T) {
	// --- given ---
	w := &MockQuotesWriter{WriteCount: 0}
	SUT := Worker{
		MarketTimeChecker: &MockTimeChecker{},
		APIClient:         &internal.MockAPIClient{},
		SymbolManager:     internal.MockSymbolsManager{},
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
