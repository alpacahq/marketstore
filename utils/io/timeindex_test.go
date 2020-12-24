package io_test

import (
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"reflect"
	"testing"
	"time"
)

func TestIndexToTime(t *testing.T) {
	t.Parallel()
	utils.InstanceConfig.Timezone = time.UTC

	tests := []struct {
		name  string
		index int64
		tf    time.Duration
		year  int16
		want  time.Time
	}{
		{
			name:  "tf=1Day, index=1, year=2020 -> 2020-01-01 00:00:00",
			index: 1, tf: 24 * time.Hour, year: 2020,
			want: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "tf=1Hour, index=1, year=2020 -> 2020-01-01 00:00:00",
			index: 1, tf: 1 * time.Hour, year: 2020,
			want: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "tf=1Sec, index=(3600*24*365), year=2019 -> 2019-12-31 23:59:59",
			index: 3600 * 24 * 365, tf: 1 * time.Second, year: 2019,
			want: time.Date(2019, 12, 31, 23, 59, 59, 0, time.UTC),
		},
		{
			name:  "tf=1Sec, index=(3600*24*(365+1)), year=2020 -> 2020-12-31 23:59:59",
			index: 3600 * 24 * (365 + 1), // +1 because 2020 is a leap year
			tf:    1 * time.Second, year: 2020,
			want: time.Date(2020, 12, 31, 23, 59, 59, 0, time.UTC),
		},
		{
			name:  "tf=1Day, index=(24*365), year=2019 -> 2019-12-31 00:00:00",
			index: 365,
			tf:    24 * time.Hour,
			year:  2019,
			want:  time.Date(2019, 12, 31, 00, 00, 00, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// convert to Time
			gotTime := io.IndexToTime(tt.index, tt.tf, tt.year)
			if !reflect.DeepEqual(gotTime, tt.want) {
				t.Fatalf("IndexToTime() = %v, want %v", gotTime, tt.want)
			}

			// convert back to Index
			gotIndex := io.TimeToIndex(gotTime, tt.tf)
			if !reflect.DeepEqual(gotIndex, tt.index) {
				t.Fatalf("TimeToIndex() = %v, want %v", gotIndex, tt.index)
			}
		})
	}
}
