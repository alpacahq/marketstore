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
func (c *Client) show(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	if !(len(args) >= 2) {
		fmt.Println("Not enough arguments, see \"\\help show\" ")
		return
	}
	tbk, start, end := c.parseQueryArgs(args)
	if tbk == nil {
		fmt.Println(`Could not parse arguments, see "\help show" `)
		return
	}

	timeStart := time.Now()

	csm, err := c.apiClient.Show(tbk, start, end)
	if err != nil {
		fmt.Println(err)
		return
	}
	elapsedTime := time.Since(timeStart)
	/*
		Should only be one symbol / file in the result, so take the first
	*/
	if len(csm.GetMetadataKeys()) == 0 {
		fmt.Println("No results")
		return
	}
	key := csm.GetMetadataKeys()[0]
	if csm[key].Len() == 0 {
		fmt.Println("No results")
		return
	}

	// print at the beginning if outputting to a file
	if c.timing && c.target != "" {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
	}

	if err = printResult(line, csm[key], c.target); err != nil {
		fmt.Println(err.Error())
	}

	// print at the end if outputting to terminal
	if c.timing && c.target == "" {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
	}
}

func (c *Client) parseQueryArgs(args []string) (tbk *io.TimeBucketKey, start, end *time.Time) {
	// args[0] must be "Symbol/Timeframe/AttributeGroup" format (e.g. "AAPL/1Min/OHLC" )
	if itemKeys := strings.Split(args[0], "/"); len(itemKeys) != 3 {
		fmt.Println(`Key is not in {Symbol/Timeframe/AttributeGroup} format (e.g. "AAPL/1Min/OHLCV)", see "\help show" `)
		return
	}
	tbk = io.NewTimeBucketKey(args[0])

	parsedTime := false
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
				fmt.Printf("Invalid time string %v\n", arg)
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
	switch len(t) {
	case 0:
		return out, fmt.Errorf("Zero length time string")
	case 10:
		return time.Parse("2006-01-02", t)
	case 16:
		return time.Parse("2006-01-02T15:04", t)
	case 18:
		return time.Parse("20060102 150405999", t)
	default:
		return out, errors.New("Invalid time format")
	}
}
