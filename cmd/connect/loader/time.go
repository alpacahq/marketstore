package loader

import (
	"strconv"
	"time"
)

func parseTime(format, dateTime string, tzLoc *time.Location, formatFixupState int) (parsedTime time.Time, err error) {

	dateString := dateTime[:len(dateTime)-formatFixupState]
	if tzLoc != nil {
		parsedTime, err = time.ParseInLocation(format, dateString, tzLoc)
		if err != nil {
			return time.Time{}, err
		}
	} else {
		parsedTime, err = time.Parse(format, dateString)
		if err != nil {
			return time.Time{}, err
		}
	}
	/*
		Attempt to use the remainder of the time field if it fits a known pattern
	*/
	switch formatFixupState {
	case 3:
		remainder := dateTime[len(dateString):]
		millis, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(millis) * time.Millisecond)
		}
	case 7:
		remainder := dateTime[len(dateString)+1:]
		micros, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(micros) * time.Microsecond)
		}
	}
	return parsedTime, nil
}
