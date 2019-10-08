package cal

import (
	"testing"
	"time"
)

func TestAddEcbHolidays(t *testing.T) {
	// The following is all of the national holidays observed by the ECB for the year 2017
	type date struct {
		day   int
		month time.Month
	}
	holidays := map[string]date{
		"good_friday": {
			day:   14,
			month: time.April,
		},
		"easter_monday": {
			day:   17,
			month: time.April,
		},
		"new_year": {
			day:   1,
			month: time.January,
		},
		"labour_day": {
			day:   1,
			month: time.May,
		},
		"christmas": {
			day:   25,
			month: time.December,
		},
		"christmas_holiday": {
			day:   26,
			month: time.December,
		},
	}

	for name, holiday := range holidays {
		t.Run(name, func(t *testing.T) {
			c := NewCalendar()
			AddEcbHolidays(c)
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
