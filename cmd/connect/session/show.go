package session

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/frontend"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

// \show [Options]
type Opts struct {
	flags.Usage

	// Destination is <symbol>/<timeframe>/<attributegroup>
	Tbk string `short:"k" long:"key" description:"TimeBucketKey, e.g. BTC/1Min/OKEX" required:"true"`
	// Lower time predicate
	EpochStart string `short:"s" long:"start" description:"Query start from datetime, e.g. 2006-01-02 or 2006-01-02T15:04 or 20060102 150405999" required:"true"`
	// Upper time predicate
	EpochEnd string `short:"e" long:"end" description:"Query end at datetime, e.g. 2006-01-02 or 2006-01-02T15:04 or 20060102 150405999"`
	// Number of max returned rows from lower/upper bound
	LimitRecordCount int `short:"n" description:"Number of max returned rows from lower/upper bound" default:"0"`
	// Set to true if LimitRecordCount should be from the lower
	LimitFromStart bool `short:"x" description:"Set to true if LimitRecordCount should be from the lower"`
	// Result export to file
	ExportToFile string `long:"export" description:"Export the result to file" value-name:"FILE"`
	// Support for functions
	Functions []string `short:"f" description:"Functions Chain"`
}

// show displays data in the date range.
func (c *Client) show(line string) {
	args := strings.Split(line, " ")
	args = args[1:]

	tbk, start, end, count, countFromStart, funcs := c.parseCommand(args)

	// tbk, start, end := c.parseQueryArgs(args)
	if tbk == nil {
		fmt.Println("Could not parse arguments, see \"\\show --help\" ")
		return
	}

	timeStart := time.Now()

	var (
		csm io.ColumnSeriesMap
		err error
	)

	if c.mode == local {
		csm, err = c.processShowLocal(tbk, start, end)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		csm, err = c.processShowRemote(tbk, start, end, count, countFromStart, funcs)
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

func (c *Client) processShowLocal(tbk *io.TimeBucketKey, start, end *time.Time) (csm io.ColumnSeriesMap, err error) {
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
		log.Error("Parsing query: %v", err)
		return
	}

	scanner, err := executor.NewReader(pr)
	if err != nil {
		log.Error("Error return from query scanner: %v", err)
		return
	}
	csm, err = scanner.Read()
	if err != nil {
		log.Error("Error return from query scanner: %v", err)
		return
	}

	return csm, nil
}

func (c *Client) processShowRemote(tbk *io.TimeBucketKey, start, end *time.Time, count int, countFromStart bool, funcs []string) (csm io.ColumnSeriesMap, err error) {
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

	if count > 0 {
		req.LimitRecordCount = &count
	}

	if countFromStart {
		req.LimitFromStart = &countFromStart
	}

	if len(funcs) > 0 {
		req.Functions = funcs
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

func (c *Client) parseCommand(args []string) (tbk *io.TimeBucketKey, start, end *time.Time, count int, countFromStart bool, funcs []string) {
	opts := Opts{}

	p := flags.NewParser(&opts, 22)
	p.Usage = ">> \\show [Options]"
	p.LongDescription = `Examples:

	Data in range: 
		>>\show -k BTC/1Min/OKEX -s 2017-03-27T12:00 -e 2017-03-27T13:00
	Export to file:
		>>\show -k BTC/1Min/OKEX -s 2017-03-27T12:00 --export=/path/to/export.csv
	Agg functions: 
		>>\show -k BTC/1Min/OKEX -s 2017-03-27T12:00 -f=Gap()`

	args, err := p.ParseArgs(args)

	if err != nil {
		return nil, nil, nil, 0, false, []string{}
	}

	tbk = io.NewTimeBucketKey(opts.Tbk)

	epochStart, err := parseTime(opts.EpochStart)
	if err != nil {
		fmt.Println("Parse query start time failed. err:", err)
		return nil, nil, nil, 0, false, []string{}
	}

	hasEnd := false
	epochEnd, err := parseTime(opts.EpochEnd)
	if err == nil {
		hasEnd = true
	}

	// options has default values
	count = opts.LimitRecordCount
	countFromStart = opts.LimitFromStart
	funcs = opts.Functions

	if len(opts.ExportToFile) > 0 {
		c.target = opts.ExportToFile
	}

	if hasEnd {
		return tbk, &epochStart, &epochEnd, count, countFromStart, funcs
	} else {
		return tbk, &epochStart, nil, count, countFromStart, funcs
	}
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
