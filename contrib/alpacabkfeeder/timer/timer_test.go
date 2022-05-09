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
		hour time.Time
		want time.Duration
	}{
		{
			"06:00 - 09:00 = 3hours",
			time.Date(1970, 1, 1, 6, 0, 0, 0, time.UTC),
			time.Date(1970, 1, 1, 9, 0, 0, 0, time.UTC),
			3 * time.Hour,
		},
		{
			"06:12:33 - 03:34:44 = 21h22m11s", // year, month, and day are not used in timeToNext func,
			time.Date(1970, 1, 1, 6, 12, 33, 0, time.UTC),
			time.Date(1970, 1, 1, 3, 34, 44, 0, time.UTC),
			21*time.Hour + 22*time.Minute + 11*time.Second,
		},
		{
			"08:00:00(JST) - 23:00:00(UTC) = 0sec", // run in Japan
			time.Date(1970, 1, 1, 8, 0, 0, 0, time.FixedZone("JST", +9*60*60)),
			time.Date(1970, 1, 1, 23, 0, 0, 0, time.UTC),
			0,
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
