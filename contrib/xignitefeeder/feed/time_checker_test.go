package feed

import (
	"testing"
	"time"
)

var ClosedDaysOfTheWeek = []time.Weekday{time.Saturday, time.Sunday}
var ClosedDays = []time.Time{
	// i.e. Marine day in Japan
	time.Date(2019, 7, 15, 0, 0, 0, 0, time.UTC),
}

// 5 minutes before the market opens in Japan (UTC)
var OpenTime = time.Date(0, 0, 0, 23, 55, 0, 0, time.UTC)

// 10 minutes after the market closes in Japan (UTC)
var CloseTime = time.Date(0, 0, 0, 6, 10, 0, 0, time.UTC)

type testCase struct {
	name   string
	arg    time.Time
	isOpen bool
}

func TestDefaultMarketTimeChecker_isOpen(t *testing.T) {
	// --- given ---
	SUT := &DefaultMarketTimeChecker{
		ClosedDaysOfTheWeek,
		ClosedDays,
		OpenTime,
		CloseTime,
	}
	// test cases
	tests := []testCase{
		{"open", // 09:00, Tuesday in JST
			time.Date(2019, 7, 16, 23, 55, 0, 0, time.UTC), true},
		{"open", // 12:00, Tuesday in JST
			time.Date(2019, 7, 16, 3, 00, 0, 0, time.UTC), true},
		{"close", // 19:00 in JST
			time.Date(2019, 7, 16, 6, 10, 0, 0, time.UTC), false},

		{"weekend", // Sunday
			time.Date(2019, 7, 7, 0, 0, 0, 0, time.UTC), false},
		{"not weekend in JST",
			time.Date(2019, 7, 7, 23, 56, 0, 0, time.UTC), true},

		{"holiday",
			time.Date(2019, 7, 15, 0, 0, 0, 0, time.UTC), false},
		{"not holiday in JST",
			time.Date(2019, 7, 15, 23, 56, 0, 0, time.UTC), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// --- when ---
			got := SUT.IsOpen(tt.arg)

			// --- then ---
			if got != tt.isOpen {
				t.Errorf("DefaultMarketTimeChecker.IsOpen() = %v, want %v", got, tt.isOpen)
			}
		})
	}
}
