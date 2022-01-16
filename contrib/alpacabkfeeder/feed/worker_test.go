package feed

import (
	"testing"
	"time"

	v2 "github.com/alpacahq/alpaca-trade-api-go/v2"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/internal"
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

// MockSnapshotWriter is a no-op QuotesWriter.
type MockSnapshotWriter struct {
	WriteCount int
}

// Write increments the counter so that a unit test could assert how many times this function is called.
func (m *MockSnapshotWriter) Write(snapshots map[string]*v2.Snapshot) error {
	m.WriteCount++
	return nil
}

func TestWorker_try_normal(t *testing.T) {
	t.Parallel()
	// --- given ---
	w := &MockSnapshotWriter{WriteCount: 0}
	SUT := Worker{
		MarketTimeChecker: &MockTimeChecker{},
		APIClient:         &internal.MockAPIClient{},
		SymbolManager:     internal.MockSymbolsManager{},
		SnapshotWriter:    w,
		Interval:          1,
	}
	// --- when ---
	if err := SUT.try(); err != nil {
		t.Errorf("error should be nil. err=%v", err)
	}

	// --- then ---
	if w.WriteCount != 1 {
		t.Errorf("write should be performed once")
	}

}
