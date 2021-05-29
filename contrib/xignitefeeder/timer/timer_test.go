package timer

import (
	"testing"
	"time"
)

func Test_timeToNext(t *testing.T) {
	t.Parallel()
	// --- given ---
	tests := []struct {
		name string
		now  time.Time
		hour int
		want time.Duration
	}{
		{"06:00 - 09:00 = 3hours",
			time.Date(1970, 1, 1, 6, 0, 0, 0, time.UTC),
			9,
			3 * time.Hour,
		},
		{"06:00 - 03:00 = 21hours",
			time.Date(1970, 1, 1, 6, 0, 0, 0, time.UTC),
			3,
			21 * time.Hour,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// --- when ---
			got := timeToNext(tt.now, tt.hour)

			// --- then ---
			if got != tt.want {
				t.Errorf("timeToNext() = %v, want %v", got, tt.want)
			}
		})
	}
}
