package session

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/frontend"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
)

// show displays data in the date range.
func (c *Client) show(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	if !(len(args) >= 2) {
		fmt.Println("Not enough arguments, see \"\\help show\" ")
		return
	}
	c.target = file
	tbk, start, end := c.parseQueryArgs(args)
	if tbk == nil {
		fmt.Println("Could not parse arguments, see \"\\help show\" ")
		return
	}

	timeStart := time.Now()
	var csm io.ColumnSeriesMap
	var err error
	if c.mode == local {
		csm, err = processShowLocal(tbk, start, end)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		csm, err = c.processShowRemote(tbk, start, end)
		if err != nil {
			fmt.Println(err)
			return
		}
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
	if c.target == file {
		writer, fileErr := newCSVWriter(tbk, start, end)
		if fileErr != nil {
			fmt.Println(fileErr)
			return
		}
		printResult(line, csm[key], writer)

	} else {
		err = printResult(line, csm[key])
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	if c.timing {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*elapsedTime.Seconds())
	}
}

// newCSVWriter returns a writer for a csv file.
func newCSVWriter(tbk *io.TimeBucketKey, start, end *time.Time) (w *csv.Writer, err error) {
	// format file name.
	var file *os.File
	if end != nil {
		file, err = os.Create(
			fmt.Sprintf("%v_%v_%v.csv",
				tbk.String(),
				start.Format("2006-01-02-15:04"),
				end.Format("2006-01-02-15:04")))
	} else {
		file, err = os.Create(
			fmt.Sprintf("%v_%v.csv", tbk.String(), *start))
		defer file.Close()
	}

	if err != nil {
		return nil, err
	}

	return csv.NewWriter(file), nil
}

func processShowLocal(tbk *io.TimeBucketKey, start, end *time.Time) (csm io.ColumnSeriesMap, err error) {
	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	query.AddTargetKey(tbk)

	if start == nil && end == nil {
		fmt.Println("No suitable date range supplied...")
		return
	} else if start != nil && end != nil {
		query.SetRange(start.Unix(), end.Unix())
	} else if end == nil {
		query.SetRange(start.Unix(), planner.MaxEpoch)
	}

	fmt.Printf("Query range: %v to %v\n", start, end)

	pr, err := query.Parse()
	if err != nil {
		fmt.Println("No results")
		Log(ERROR, "Parsing query: %v", err)
		return
	}

	scanner, err := executor.NewReader(pr)
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}
	csm, _, err = scanner.Read()
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}

	return csm, nil
}

func (c *Client) processShowRemote(tbk *io.TimeBucketKey, start, end *time.Time) (csm io.ColumnSeriesMap, err error) {
	if end == nil {
		t := time.Unix(planner.MaxEpoch, 0)
		end = &t
	}
	epochStart := start.UTC().Unix()
	epochEnd := end.UTC().Unix()
	req := frontend.QueryRequest{
		IsSQLStatement: false,
		SQLStatement:   string(0),
		Destination:    tbk.String(),
		EpochStart:     &epochStart,
		EpochEnd:       &epochEnd,
	}
	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{req},
	}

	resp, err := c.rc.DoRPC("Query", args)
	if err != nil {
		return nil, err
	}

	return *resp.(*io.ColumnSeriesMap), nil
}

func (c *Client) parseQueryArgs(args []string) (tbk *io.TimeBucketKey, start, end *time.Time) {
	tbk = io.NewTimeBucketKey(args[0])
	if tbk == nil {
		fmt.Println("Key is not in proper format, see \"\\help show\" ")
		return
	}
	parsedTime := false
	for _, arg := range args[1:] {
		switch strings.ToLower(arg) {
		case "between":
		case "and":
		case "csv":
			c.target = file
		default:
			t, err := parseTime(arg)
			if err != nil {
				Log(ERROR, "Invalid Symbol/Timeframe/recordFormat string %v", arg)
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
