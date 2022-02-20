package feed

import (
	"testing"
	"time"
)

var (
	ClosedDaysOfTheWeek = []time.Weekday{time.Saturday, time.Sunday}
	ClosedDays          = []time.Time{
		// Marine day in Japan
		time.Date(2019, 7, 15, 0, 0, 0, 0, time.UTC),
		// Health and Sports day in Japan
		time.Date(2019, 10, 14, 0, 0, 0, 0, time.UTC),
	}
)

// 5 minutes before the market opens in Japan (UTC).
var OpenTime = time.Date(0, 0, 0, 23, 55, 0, 0, time.UTC)

// 10 minutes after the market closes in Japan (UTC).
var CloseTime = time.Date(0, 0, 0, 6, 10, 0, 0, time.UTC)

type testCase struct {
	name   string
	arg    time.Time
	isOpen bool
}

func TestDefaultMarketTimeChecker_isOpen(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultMarketTimeChecker{
		ClosedDaysOfTheWeek,
		ClosedDays,
		OpenTime,
		CloseTime,
	}
	// test cases
	tests := []testCase{
		{
			"open", // 09:00, Tuesday in JST
			time.Date(2019, 7, 16, 23, 55, 0, 0, time.UTC), true,
		},
		{
			"open", // 12:00, Tuesday in JST
			time.Date(2019, 7, 16, 3, 0, 0, 0, time.UTC), true,
		},
		{
			"close", // 19:00 in JST
			time.Date(2019, 7, 16, 6, 10, 0, 0, time.UTC), false,
		},

		{
			"weekend", // Sunday
			time.Date(2019, 7, 7, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"not weekend in JST",
			time.Date(2019, 7, 7, 23, 56, 0, 0, time.UTC), true,
		},

		{
			"holiday",
			time.Date(2019, 7, 15, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"not holiday in JST",
			time.Date(2019, 7, 15, 23, 56, 0, 0, time.UTC), true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// --- when ---
			got := SUT.IsOpen(tt.arg)

			// --- then ---
			if got != tt.isOpen {
				t.Errorf("DefaultMarketTimeChecker.IsOpen() = %v, want %v", got, tt.isOpen)
			}
		})
	}
}

type subTestCase struct {
	name         string
	currentTime  time.Time
	businessDays int
	expected     time.Time
	isErr        bool
}

func TestDefaultMarketTimeChecker_Sub(t *testing.T) {
	t.Parallel()
	// --- given ---
	SUT := &DefaultMarketTimeChecker{
		ClosedDaysOfTheWeek,
		ClosedDays,
		OpenTime,
		CloseTime,
	}
	// test cases
	tests := []subTestCase{
		{
			"3 business days", // 2019-10-18 = Friday
			time.Date(2019, 10, 18, 0, 0, 0, 0, time.UTC), 3,
			time.Date(2019, 10, 15, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"Sunday and Saturday are not business days", // 2019-10-21 = Monday
			time.Date(2019, 10, 21, 0, 0, 0, 0, time.UTC), 3,
			time.Date(2019, 10, 16, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"10/14(Mon) is a national holiday",
			time.Date(2019, 10, 15, 0, 0, 0, 0, time.UTC), 3,
			time.Date(2019, 10, 9, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"We consider Friday is one business-day before Sunday", // 2019-10-18 = Friday
			time.Date(2019, 10, 21, 0, 0, 0, 0, time.UTC), 1,
			time.Date(2019, 10, 18, 0, 0, 0, 0, time.UTC), false,
		},
		{
			"businessDays argument should be a positive integer",
			time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), -1,
			time.Time{},
			true,
		},
		{
			"Current date is returned when businessDays = 0",
			time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), 0,
			time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// --- when ---
			got, err := SUT.Sub(tt.currentTime, tt.businessDays)

			// --- then ---
			if got != tt.expected {
				t.Errorf("DefaultMarketTimeChecker.Sub() = %v, want %v", got, tt.expected)
			}
			if (err != nil) != tt.isErr {
				t.Errorf("DefaultMarketTimeChecker.Sub() returned %v error", err)
			}
		})
	}
}
