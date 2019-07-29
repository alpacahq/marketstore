package loader

import (
	"fmt"
	"time"
)

// readTimeColumns retuns the epoch and nano columns of a csv file.
func readTimeColumns(csvData [][]string, columnIndex []int, conf *CSVConfig) (epochCol []int64, nanosCol []int32) {
	var err error
	epochCol = make([]int64, len(csvData))
	nanosCol = make([]int32, len(csvData))
	/*
		Now we can calculate which fields are present that define the Epoch - we either have a pre-defined Epoch
		or we must compose one from a date and time
	*/
	mustComposeEpoch := columnIndex[2] == -1
	if mustComposeEpoch {
		if columnIndex[0] == -1 || columnIndex[1] == -1 {
			fmt.Println("Unable to build Epoch time from mapping - need both a date and time")
			return nil, nil
		}
	}

	/*
		Obtain the time index values
	*/
	// var tzLoc *time.Location
	var tzLoc *time.Location
	if len(conf.Timezone) != 0 {
		tzLoc, err = time.LoadLocation(conf.Timezone)
		if err != nil {
			fmt.Printf("Unable to parse timezone %s: %s\n", conf.Timezone, err.Error())
			return nil, nil
		}
	}

	var dateTime string
	var rowTime time.Time
	firstParse := true
	var formatAdj int
	for i, row := range csvData {
		if mustComposeEpoch {
			rowDateIdx := columnIndex[0]
			rowTimeIdx := columnIndex[1]
			dateTime = row[rowDateIdx] + " " + row[rowTimeIdx]
		} else {
			dateTime = row[columnIndex[2]]
		}
		rowTime, err = parseTime(conf.TimeFormat, dateTime, tzLoc, formatAdj)
		if firstParse && err != nil {
			// Attempt to "tune" the time format
			formatAdj = len(dateTime) - len(conf.TimeFormat)
			if formatAdj > 0 {
				rowTime, err = parseTime(conf.TimeFormat, dateTime, tzLoc, formatAdj)
			}
			firstParse = false
		}
		if err != nil {
			fmt.Printf("Error parsing Epoch column(s) from input data file: %s\n", err.Error())
			fmt.Printf("rowTime %v, mustComposeEpoch %v, dateTime %v, format %v, loc %v, fromAdj %v", rowTime, mustComposeEpoch, dateTime, conf.TimeFormat, tzLoc, formatAdj)
			return nil, nil
		}
		epochCol[i] = rowTime.UTC().Unix()
		nanosCol[i] = int32(rowTime.UTC().Nanosecond())
	}

	return epochCol, nanosCol
}
