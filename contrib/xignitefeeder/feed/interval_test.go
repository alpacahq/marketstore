package feed_test

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/feed"
)

var (
	jst                      = time.FixedZone("Asia/Tokyo", 9*60*60)
	exampleDate              = time.Date(2021, 8, 20, 0, 0, 0, 0, time.UTC)
	exampleDatePlus5min      = time.Date(2021, 8, 20, 0, 5, 0, 0, time.UTC)
	exampleDatePlus4min59s   = time.Date(2021, 8, 20, 0, 4, 59, 0, time.UTC)
	exampleDatePlus5minInJST = time.Date(2021, 8, 20, 9, 5, 0, 0, jst)
)

type mockMarketTimeChecker struct {
	isOpen bool
}

func (m *mockMarketTimeChecker) IsOpen(_ time.Time) bool {
	return m.isOpen
}
func (m *mockMarketTimeChecker) Sub(_ time.Time, _ int) (time.Time, error) {
	panic("not implemented")
}

func TestIntervalMarketTimeChecker_IsOpen(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		MarketTimeChecker feed.MarketTimeChecker
		Interval          time.Duration
		CurrentTime       time.Time
		LastTime          time.Time
		want              bool
	}{
		"ok: IsOpen returns true if the interval elapsed": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			Interval:          5 * time.Minute,
			LastTime:          exampleDate,
			CurrentTime:       exampleDatePlus5min,
			want:              true,
		},
		"ok: IsOpen returns false if the interval has not yet elapsed": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			Interval:          5 * time.Minute,
			LastTime:          exampleDate,
			CurrentTime:       exampleDatePlus4min59s,
			want:              false,
		},
		"ok: time in any location can be passed": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			Interval:          5 * time.Minute,
			LastTime:          exampleDate,
			CurrentTime:       exampleDatePlus5minInJST,
			want:              true,
		},
		"ok: always Open if the base market time checker is IsOpen=true": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: true},
			Interval:          5 * time.Minute,
			LastTime:          exampleDate,
			CurrentTime:       exampleDate,
			want:              true,
		},
	}
	for name := range tests {
		tt := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			c := feed.NewIntervalMarketTimeChecker(
				tt.MarketTimeChecker,
				tt.Interval,
			)
			c.LastTime = tt.LastTime

			// --- when ---
			got := c.IsOpen(tt.CurrentTime)

			// --- then ---
			if got != tt.want {
				t.Errorf("IsOpen() = %v, want %v", got, tt.want)
			}
		})
	}
}
