package cal

import (
	"testing"
	"time"
)

func TestAddUSHolidays(t *testing.T) {
	// The following is all of the national holidays in US for the year 2017
	type date struct {
		day   int
		month time.Month
	}
	holidays := map[string]date{
		"new_year": {
			day:   1,
			month: time.January,
		},
		"mlk": {
			day:   16,
			month: time.January,
		},
		"presidents": {
			day:   20,
			month: time.February,
		},
		"memorial": {
			day:   29,
			month: time.May,
		},
		"independence": {
			day:   4,
			month: time.July,
		},
		"labor": {
			day:   4,
			month: time.September,
		},
		"columbus": {
			day:   9,
			month: time.October,
		},
		"veterans": {
			day:   11,
			month: time.November,
		},
		"thanksgiving": {
			day:   23,
			month: time.November,
		},
		"christmas": {
			day:   25,
			month: time.December,
		},
	}

	for name, holiday := range holidays {
		t.Run(name, func(t *testing.T) {
			c := NewCalendar()
			AddUsHolidays(c)
			i := time.Date(2017, holiday.month, holiday.day, 0, 0, 0, 0, time.UTC)

			if !c.IsHoliday(i) {
				t.Errorf("Expected %q to be a holiday but wasn't", i)
			}
			if c.IsWorkday(i) {
				t.Errorf("Did not expect %q to be a holiday", i)
			}
		})
	}
}
