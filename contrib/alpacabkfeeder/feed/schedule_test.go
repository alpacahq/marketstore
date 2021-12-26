package feed_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/feed"
)

func TestParseSchedule(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		s       string
		want    []int
		wantErr bool
	}{
		{
			name:    "\"0,15,30,45\" -> every 15 minutes",
			s:       "0,15,30,45",
			want:    []int{0, 15, 30, 45},
			wantErr: false,
		},
		{
			name:    "\"50,10\" -> 10 and 50, numbers are sorted",
			s:       "50,10",
			want:    []int{10, 50},
			wantErr: false,
		},
		{
			name:    "whitespaces are ignored",
			s:       "   20,   40    ",
			want:    []int{20, 40},
			wantErr: false,
		},
		{
			name:    "no schedule is set",
			s:       "",
			want:    []int{},
			wantErr: false,
		},
		{
			name:    "NG/minute must be between [0, 59]",
			s:       "100",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "NG/range is not supported",
			s:       "0-10",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := feed.ParseSchedule(tt.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSchedule() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScheduledMarketTimeChecker_IsOpen(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		MarketTimeChecker feed.MarketTimeChecker
		ScheduleMin       []int
		CurrentTime       time.Time
		LastTime          time.Time
		want              bool
	}{
		"ok: run - 00:15 matches {0} in the schedule": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			ScheduleMin:       []int{0, 15, 30, 45},
			LastTime:          time.Time{},
			CurrentTime:       time.Date(2021, 8, 20, 0, 15, 0, 0, time.UTC),
			want:              true,
		},
		"ok: not run - 00:01 does not match any of {0,15,30,45}": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			ScheduleMin:       []int{0, 15, 30, 45},
			LastTime:          time.Time{},
			CurrentTime:       time.Date(2021, 8, 20, 0, 1, 0, 0, time.UTC),
			want:              false,
		},
		"ok: not run - run only once per minute": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: false},
			ScheduleMin:       []int{20},
			LastTime:          time.Date(2021, 8, 20, 0, 19, 30, 0, time.UTC),
			CurrentTime:       time.Date(2021, 8, 20, 0, 20, 0, 0, time.UTC),
			want:              false,
		},
		"ok: run - always run when the original market time checker's IsOpen=true": {
			MarketTimeChecker: &mockMarketTimeChecker{isOpen: true},
			ScheduleMin:       []int{20},
			LastTime:          time.Time{},
			CurrentTime:       time.Date(2021, 8, 20, 0, 0, 0, 0, time.UTC),
			want:              true,
		},
	}
	for name := range tests {
		tt := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			c := feed.NewScheduledMarketTimeChecker(
				tt.MarketTimeChecker,
				tt.ScheduleMin,
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
