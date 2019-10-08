// (c) 2017 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import (
	"testing"
	"time"
)

func TestCalculateEaster(t *testing.T) {
	tests := []struct {
		t    time.Time
		want bool
	}{
		{time.Date(2016, 3, 27, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2017, 4, 16, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2018, 4, 1, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2019, 4, 21, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2020, 4, 12, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2021, 4, 4, 0, 0, 0, 0, time.UTC), true},
	}

	for _, test := range tests {
		easter := calculateEaster(test.t.Year(), test.t.Location())
		got := (test.t == easter)
		if got != test.want {
			t.Errorf("got: %t; want: %t (%s)", got, test.want, test.t)
		}
	}
}

func TestCalculateGoodFriday(t *testing.T) {
	c := NewCalendar()
	c.AddHoliday(ECBGoodFriday)

	tests := []struct {
		t    time.Time
		want bool
	}{
		{time.Date(2016, 3, 25, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2017, 4, 14, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2018, 3, 30, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2019, 4, 19, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2020, 4, 10, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2021, 4, 2, 0, 0, 0, 0, time.UTC), true},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t; want: %t (%s)", got, test.want, test.t)
		}
	}
}

func TestCalculateEasterMonday(t *testing.T) {
	c := NewCalendar()
	c.AddHoliday(ECBEasterMonday)

	tests := []struct {
		t    time.Time
		want bool
	}{
		{time.Date(2016, 3, 28, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2017, 4, 17, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2018, 4, 2, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2019, 4, 22, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2020, 4, 13, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2021, 4, 5, 0, 0, 0, 0, time.UTC), true},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t; want: %t (%s)", got, test.want, test.t)
		}
	}
}
