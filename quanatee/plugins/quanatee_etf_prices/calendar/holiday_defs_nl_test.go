package cal

import (
	"testing"
	"time"
)

func TestDutchHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddDutchHolidays(c)

	tests := []struct {
		t    time.Time
		want bool
	}{
		{time.Date(2014, 4, 27, 12, 0, 0, 0, time.UTC), false}, // Koningsdag (27th on a Sunday)
		{time.Date(2014, 4, 26, 12, 0, 0, 0, time.UTC), true},  // Koningsdag (26th in 2014)
		{time.Date(2017, 4, 27, 12, 0, 0, 0, time.UTC), true},  // Koningsdag (27th in 2017)
		{time.Date(2017, 5, 5, 12, 0, 0, 0, time.UTC), true},   // Bevrijdingsdag
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t; want: %t (%s)", got, test.want, test.t)
		}
	}
}
