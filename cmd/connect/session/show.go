package session

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// show displays data in the date range.
func (c *Client) show(line string) error {
	args := strings.Split(line, " ")
	args = args[1:]
	// need bucket name and date
	const argLen = 1
	if !(len(args) > argLen) {
		log.Error(`Not enough arguments, see '\help show'`)
		return nil
	}
	tbk, start, end := c.parseQueryArgs(args)
	if tbk == nil {
		log.Error(`Could not parse arguments, see "\help show" `)
		return nil
	}

	timeStart := time.Now()

	csm, err := c.apiClient.Show(tbk, start, end)
	if err != nil {
		log.Error(err.Error())
		return fmt.Errorf("show command failed: %w", err)
	}
	elapsedTime := time.Since(timeStart)
	/*
		Should only be one symbol / file in the result, so take the first
	*/
	if len(csm.GetMetadataKeys()) == 0 {
		log.Info("No results")
		return nil
	}
	key := csm.GetMetadataKeys()[0]
	if csm[key].Len() == 0 {
		log.Info("No results")
		return nil
	}

	// print at the beginning if outputting to a file
	if c.printExecutionTime && c.target != "" {
		log.Info("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
	}

	if err = printResult(line, csm[key], c.target); err != nil {
		log.Error(err.Error())
	}

	// print at the end if outputting to terminal
	if c.printExecutionTime && c.target == "" {
		log.Info("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
	}
	return nil
}

func (c *Client) parseQueryArgs(args []string) (tbk *io.TimeBucketKey, start, end *time.Time) {
	// args[0] must be "Symbol/Timeframe/AttributeGroup" format (e.g. "AAPL/1Min/OHLC" )
	const itemKeyLen = 3
	if itemKeys := strings.Split(args[0], "/"); len(itemKeys) != itemKeyLen {
		log.Error(`Key is not in {Symbol/Timeframe/AttributeGroup} format (e.g. "AAPL/1Min/OHLCV)", see "\help show" `)
		return
	}
	tbk = io.NewTimeBucketKey(args[0])

	var parsedTime bool
	for _, arg := range args[1:] {
		switch strings.ToLower(arg) {
		case "between":
		case "and":
		case "csv":
			c.target = "mstore-csv-output.csv"
		default:
			t, err := parseTime(arg)
			if err != nil {
				log.Error("Invalid Symbol/Timeframe/recordFormat string %v", arg)
				log.Error("Invalid time string %v\n", arg)
				return nil, nil, nil
			}
			if parsedTime {
				end = &t
			} else {
				start = &t
				parsedTime = true
			}
		}
	}

	if parsedTime {
		return tbk, start, end
	}

	return nil, nil, nil
}

func parseTime(t string) (out time.Time, err error) {
	/*
		Implements a variety of format choices that key on string length
	*/
	const (
		formatLen1 = 10 // = len("2006-01-02")
		formatLen2 = 16 // = len("2006-01-02T15:04")
		formatLen3 = 18 // = len("20060102 150405999")
	)
	switch len(t) {
	case 0:
		return out, fmt.Errorf("zero length time string")
	case formatLen1:
		return time.Parse("2006-01-02", t)
	case formatLen2:
		return time.Parse("2006-01-02T15:04", t)
	case formatLen3:
		return time.Parse("20060102 150405999", t)
	default:
		return out, errors.New("invalid time format")
	}
}
