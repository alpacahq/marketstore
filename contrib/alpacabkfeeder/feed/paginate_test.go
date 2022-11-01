package feed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func date(day int) time.Time {
	const (
		year  = 2021
		month = 12
	)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func Test_datePageIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		pageDays int
		want     []dateRange
	}{
		{
			name:     "ok/5 days paginated by pageSize=2",
			start:    date(1),
			end:      date(5),
			pageDays: 2,
			want: []dateRange{
				{From: date(1), To: date(3)},
				{From: date(3), To: date(5)},
			},
		},
		{
			name:     "ok/3 days paginated by pageSize=5",
			start:    date(1),
			end:      date(3),
			pageDays: 5,
			want: []dateRange{
				{From: date(1), To: date(3)},
			},
		},
		{
			name:     "ok/3 days paginated by pageSize=1",
			start:    date(1),
			end:      date(3),
			pageDays: 1,
			want: []dateRange{
				{From: date(1), To: date(2)},
				{From: date(2), To: date(3)},
				{From: date(3), To: date(4)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			i := 0
			for dateRange := range datePageIndex(tt.start, tt.end, tt.pageDays) {
				require.Equal(t, tt.want[i], dateRange)
				i++
			}
		})
	}
}
