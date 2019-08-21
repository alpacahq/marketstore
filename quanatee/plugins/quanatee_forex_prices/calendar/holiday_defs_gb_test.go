package cal

import (
	"testing"
	"time"
)

func TestAddBritishHolidays(t *testing.T) {
	// The following is all of the national holidays in GB for the year 2017
	type date struct {
		day   int
		month time.Month
	}
	holidays := map[string]date{
		"new_year": {
			day:   2,
			month: time.January,
		},
		"good_friday": {
			day:   14,
			month: time.April,
		},
		"easter_monday": {
			day:   17,
			month: time.April,
		},
		"early_may": {
			day:   1,
			month: time.May,
		},
		"spring": {
			day:   29,
			month: time.May,
		},
		"summer": {
			day:   28,
			month: time.August,
		},
		"christmas": {
			day:   25,
			month: time.December,
		},
		"boxing": {
			day:   26,
			month: time.December,
		},
	}

	for name, holiday := range holidays {
		t.Run(name, func(t *testing.T) {
			c := NewCalendar()
			AddBritishHolidays(c)
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

func TestCalculateNewYearsHoliday(t *testing.T) {
	c := NewCalendar()
	AddBritishHolidays(c)

	i := time.Date(2011, time.January, 3, 0, 0, 0, 0, time.UTC)

	if !c.IsHoliday(i) {
		t.Errorf("Expected %q to be a holiday but wasn't", i)
	}
	if c.IsWorkday(i) {
		t.Errorf("Did not expect %q to be a holiday", i)
	}
}
