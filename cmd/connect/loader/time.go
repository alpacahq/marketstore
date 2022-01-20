package loader

import (
	"math"
	"strconv"
	"strings"
	"time"
)

func parseTime(format, dateTime string, tzLoc *time.Location, formatFixupState int) (parsedTime time.Time, err error) {
	tz := time.UTC
	if tzLoc != nil {
		tz = tzLoc
	}
	dateString := dateTime[:len(dateTime)-formatFixupState]
	if format == "timestamp" {
		parts := strings.Split(dateTime, ".")
		sec, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}

		nsec := (int64)(0)
		if len(parts) > 1 {
			nsec, err = strconv.ParseInt(parts[1], 10, 64)
			if err == nil {
				nsec = (int64)(math.Pow10(9-len(parts[1]))) * nsec
			} else {
				return time.Time{}, err
			}
		}

		parsedTime = time.Unix(sec, nsec).In(tzLoc)
		formatFixupState = 0
	} else {
		parsedTime, err = time.ParseInLocation(format, dateString, tz)
		if err != nil {
			return time.Time{}, err
		}
	}

	// Attempt to use the remainder of the time field if it fits a known pattern
	// if the dateTime string has a suffix like "20161230 21:37:57 14", extract " 14" and consider it a millisec.
	// if the suffix is like " 140000", consider it a microsec.
	const millisecSuffixLen = 3
	const microsecSuffixLen = 7
	switch formatFixupState {
	case millisecSuffixLen:
		remainder := dateTime[len(dateString):]
		millis, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(millis) * time.Millisecond)
		}
	case microsecSuffixLen:
		remainder := dateTime[len(dateString)+1:]
		micros, err := strconv.ParseInt(remainder, 10, 64)
		if err == nil {
			parsedTime = parsedTime.Add(time.Duration(micros) * time.Microsecond)
		}
	}
	return parsedTime, nil
}
