package feed_test

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
)

var (
	ClosedDaysOfTheWeek = []time.Weekday{time.Saturday, time.Sunday}
	ClosedDays          = []time.Time{
		// Independence day
		time.Date(2019, 7, 4, 0, 0, 0, 0, time.UTC),
	}
	ny, _ = time.LoadLocation("America/New_York")
	jst   = time.FixedZone("Asia/Tokyo", 9*60*60)
)

type testCase struct {
	name                   string
	arg                    time.Time
	openHour, openMinute   int
	closeHour, closeMinute int
	wantIsOpen             bool
}

func TestDefaultMarketTimeChecker_isOpen(t *testing.T) {
	t.Parallel()

	// test cases
	tests := []testCase{
		// Sunday, 13 March 2022, 02:00:00 clocks were turned forward 1 hour to
		// Sunday, 13 March 2022, 03:00:00 local daylight time instead.
		// Sunday, 6 November 2022, 02:00:00 clocks are turned backward 1 hour to
		// Sunday, 6 November 2022, 01:00:00 local standard time instead.
		// 2019-07-14 = Sunday, EDT(UTC-0400)
		// 2019-07-16 = Tuesday, EDT(UTC-0400)
		// 2019-07-20 = Saturday, EDT(UTC-0400)
		{
			name:     "open(2019-03-19(Tuesday) 20:00UTC = 2019-03-19 16:00EDT)",
			arg:      time.Date(2019, 3, 19, 20, 0o0, 0, 0, time.UTC),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: true,
		},
		{
			name:     "open(2019-03-19(Tuesday) 13:30UTC = 2019-03-19 9:30EDT)",
			arg:      time.Date(2019, 3, 19, 13, 30, 0, 0, time.UTC),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: true,
		},
		{
			name:     "close(2019-03-19(Tuesday) 13:29UTC = 2019-03-19 9:29EDT)",
			arg:      time.Date(2019, 3, 19, 13, 29, 0, 0, time.UTC),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: false,
		},
		{
			name:     "close(2019-01-15(Tuesday) 06:00JST = 2019-03-18 16:01EST)",
			arg:      time.Date(2019, 1, 15, 0o6, 0o1, 0, 0, jst),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: false,
		},
		{
			name:     "open(2019-01-15(Tuesday) 14:30UTC = 2019-03-19 09:30EST)",
			arg:      time.Date(2019, 1, 15, 14, 30, 0, 0, time.UTC),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: true,
		},
		{
			name:     "open(2019-07-20 06:00 is Saturday in JST but 2019-07-19 16:00 is Friday in EDT",
			arg:      time.Date(2019, 7, 20, 5, 0, 0, 0, jst),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: true,
		},
		{
			name:     "close(weekend. 2019-07-07 is Sunday)",
			arg:      time.Date(2019, 7, 7, 0, 0, 0, 0, time.UTC),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: false,
		},
		{
			name:     "close(holiday. 2019-07-04 is Monday but Independence day)",
			arg:      time.Date(2019, 7, 4, 12, 0, 0, 0, ny),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: false,
		},
		{
			name:     "close(holiday. 2019-07-05 is not a national holiday in JST but Independence day in US)",
			arg:      time.Date(2019, 7, 5, 2, 0, 0, 0, jst),
			openHour: 9, openMinute: 30,
			closeHour: 16, closeMinute: 0,
			wantIsOpen: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			SUT := &feed.DefaultMarketTimeChecker{
				ClosedDaysOfTheWeek: ClosedDaysOfTheWeek,
				ClosedDays:          ClosedDays,
				OpenHourNY:          tt.openHour, OpenMinuteNY: tt.openMinute,
				CloseHourNY: tt.closeHour, CloseMinuteNY: tt.closeMinute,
			}

			// --- when ---
			got := SUT.IsOpen(tt.arg)

			// --- then ---
			if got != tt.wantIsOpen {
				t.Errorf("DefaultMarketTimeChecker.IsOpen() = %v, want %v", got, tt.wantIsOpen)
			}
		})
	}
}
