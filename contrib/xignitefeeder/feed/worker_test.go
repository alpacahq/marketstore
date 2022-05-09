package feed

import (
	"context"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/internal"
)

// MockTimeChecker always returns Open.
type MockTimeChecker struct{}

// IsOpen always returns Open.
func (m *MockTimeChecker) IsOpen(t time.Time) bool {
	return true
}

// Sub always returns the same date as the first argument.
func (m *MockTimeChecker) Sub(t time.Time, d int) (time.Time, error) {
	return t, nil
}

// MockQuotesWriter is a no-op QuotesWriter.
type MockQuotesWriter struct {
	WriteCount int
}

// Write increments the counter so that a unit test could assert how many times this function is called.
func (m *MockQuotesWriter) Write(resp api.GetQuotesResponse) error {
	m.WriteCount++
	return nil
}

func TestWorker_try_normal(t *testing.T) {
	t.Parallel()
	// --- given ---
	w := &MockQuotesWriter{WriteCount: 0}
	SUT := Worker{
		MarketTimeChecker: &MockTimeChecker{},
		APIClient:         &internal.MockAPIClient{},
		SymbolManager:     internal.MockSymbolsManager{},
		QuotesWriter:      w,
		Interval:          1,
	}
	// --- when & then ---
	if err := SUT.try(context.Background()); err != nil {
		t.Errorf("error should be nil. err=%v", err)
	}

	// --- then ---
	if w.WriteCount != 1 {
		t.Errorf("write should be performed once")
	}
}
