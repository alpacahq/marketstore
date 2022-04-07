package frontend

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// This is the parameter interface for DataService.Query method.
type QueryRequest struct {
	// Note: SQL is not fully supported
	IsSQLStatement bool   `msgpack:"is_sqlstatement"` // If this is a SQL request, Only SQLStatement is relevant
	SQLStatement   string `msgpack:"sql_statement"`

	// Destination is <symbol>/<timeframe>/<attributegroup>
	Destination string `msgpack:"destination"`
	// This is not usually set, defaults to Symbol/Timeframe/AttributeGroup
	KeyCategory string `msgpack:"key_category,omitempty"`
	// Lower time predicate (i.e. index >= start) in unix epoch second
	EpochStart *int64 `msgpack:"epoch_start,omitempty"`
	// Nanosecond of the lower time predicate
	EpochStartNanos *int64 `msgpack:"epoch_start_nanos,omitempty"`
	// Upper time predicate (i.e. index <= end) in unix epoch second
	EpochEnd *int64 `msgpack:"epoch_end,omitempty"`
	// Nanosecond of the upper time predicate
	EpochEndNanos *int64 `msgpack:"epoch_end_nanos,omitempty"`
	// Number of max returned rows from lower/upper bound
	LimitRecordCount *int `msgpack:"limit_record_count,omitempty"`
	// Set to true if LimitRecordCount should be from the lower
	LimitFromStart *bool `msgpack:"limit_from_start,omitempty"`
	// Array of column names to be returned
	Columns []string `msgpack:"columns,omitempty"`

	// Support for functions is experimental and subject to change
	Functions []string `msgpack:"functions,omitempty"`
}

type MultiQueryRequest struct {
	/*
		A multi-request allows for different Timeframes and record formats for each request
	*/
	Requests []QueryRequest `msgpack:"requests"`
}

type QueryResponse struct {
	Result *io.NumpyMultiDataset `msgpack:"result"`
}

type MultiQueryResponse struct {
	Responses []QueryResponse `msgpack:"responses"`
	Version   string          `msgpack:"version"`  // Server Version
	Timezone  string          `msgpack:"timezone"` // Server Timezone
}

// ToColumnSeriesMap converts a MultiQueryResponse to a
// ColumnSeriesMap, returning an error if there is any
// issue encountered while converting.
func (resp *MultiQueryResponse) ToColumnSeriesMap() (*io.ColumnSeriesMap, error) {
	if resp == nil {
		return nil, nil
	}

	csm := io.NewColumnSeriesMap()

	for _, ds := range resp.Responses { // Datasets are packed in a slice, each has a NumpyMultiDataset inside
		nmds := ds.Result
		for tbkStr, startIndex := range nmds.StartIndex {
			cs, err := nmds.ToColumnSeries(startIndex, nmds.Lengths[tbkStr])
			if err != nil {
				return nil, err
			}
			tbk := io.NewTimeBucketKeyFromString(tbkStr)
			csm[*tbk] = cs
		}
	}

	return &csm, nil
}

func (s *DataService) Query(r *http.Request, reqs *MultiQueryRequest, response *MultiQueryResponse) (err error) {
	response.Version = utils.GitHash
	response.Timezone = utils.InstanceConfig.Timezone.String()
	for i := range reqs.Requests {
		var (
			resp *QueryResponse
			err  error
		)
		// SQL
		if reqs.Requests[i].IsSQLStatement {
			resp, err = s.executeSQL(reqs.Requests[i].SQLStatement)
			if err != nil {
				return err
			}
		} else {
			// Query
			resp, err = s.executeQuery(&reqs.Requests[i])
			if err != nil {
				return err
			}
		}

		response.Responses = append(response.Responses, *resp)
	}
	return nil
}

func (s *DataService) executeSQL(sqlStatement string) (*QueryResponse, error) {
	queryTree, err := sqlparser.BuildQueryTree(sqlStatement)
	if err != nil {
		return nil, err
	}
	es, err := sqlparser.NewExecutableStatement(queryTree)
	if err != nil {
		return nil, err
	}
	cs, err := es.Materialize(s.aggRunner, s.catalogDir)
	if err != nil {
		return nil, err
	}
	nds, err := io.NewNumpyDataset(cs)
	if err != nil {
		return nil, err
	}
	tbk := io.NewTimeBucketKeyFromString(sqlStatement + ":SQL")
	nmds, err := io.NewNumpyMultiDataset(nds, *tbk)
	if err != nil {
		return nil, err
	}
	return &QueryResponse{nmds}, nil
}

func (s *DataService) executeQuery(req *QueryRequest) (*QueryResponse, error) {
	/*
		Assumption: Within each TimeBucketKey, we have one or more of each category, with the exception of
		the AttributeGroup (aka Record Format) and Timeframe
		Within each TimeBucketKey in the request, we allow for a comma separated list of items, e.g.:
			destination1.items := "TSLA,AAPL,CG/1Min/OHLCV"
		Constraints:
		- If there is more than one record format in a single destination, we return an error
		- If there is more than one Timeframe in a single destination, we return an error
	*/
	dest := io.NewTimeBucketKey(req.Destination, req.KeyCategory)
	/*
		All destinations in a request must share the same record format (AttributeGroup) and Timeframe
	*/
	RecordFormat := dest.GetItemInCategory("AttributeGroup")
	Timeframe := dest.GetItemInCategory("Timeframe")
	Symbols := dest.GetMultiItemInCategory("Symbol")

	if len(Timeframe) == 0 || len(RecordFormat) == 0 || len(Symbols) == 0 {
		return nil, fmt.Errorf("destinations must have a Symbol, Timeframe and AttributeGroup, have: %s",
			dest.String())
	} else if len(Symbols) == 1 && Symbols[0] == "*" {
		// replace the * "symbol" with a list all known actual symbols
		allSymbols := s.catalogDir.GatherCategoriesAndItems()["Symbol"]
		symbols := make([]string, 0, len(allSymbols))
		for symbol := range allSymbols {
			symbols = append(symbols, symbol)
		}
		keyParts := []string{strings.Join(symbols, ","), Timeframe, RecordFormat}
		itemKey := strings.Join(keyParts, "/")
		dest = io.NewTimeBucketKey(itemKey, req.KeyCategory)
	}

	epochStart := int64(0)
	epochEnd := int64(math.MaxInt64)
	var epochStartNanos, epochEndNanos int64
	if req.EpochStart != nil {
		epochStart = *req.EpochStart
		if req.EpochStartNanos != nil {
			epochStartNanos = *req.EpochStartNanos
		}
	}
	if req.EpochEnd != nil {
		epochEnd = *req.EpochEnd
		if req.EpochEndNanos != nil {
			epochEndNanos = *req.EpochEndNanos
		}
	}
	limitRecordCount := 0
	if req.LimitRecordCount != nil {
		limitRecordCount = *req.LimitRecordCount
	}
	limitFromStart := false
	if req.LimitFromStart != nil {
		limitFromStart = *req.LimitFromStart
	}
	columns := make([]string, 0)
	if req.Columns != nil {
		columns = req.Columns
	}

	start := io.ToSystemTimezone(time.Unix(epochStart, epochStartNanos))
	end := io.ToSystemTimezone(time.Unix(epochEnd, epochEndNanos))
	csm, err := s.query.ExecuteQuery(
		dest,
		start, end,
		limitRecordCount, limitFromStart,
		columns,
	)
	if err != nil {
		return nil, err
	}

	/*
		Execute function pipeline, if requested
	*/
	if len(req.Functions) != 0 {
		for tbkStr, cs := range csm {
			csOut, err := s.aggRunner.Run(req.Functions, cs, tbkStr)
			if err != nil {
				return nil, err
			}
			csm[tbkStr] = csOut
		}
	}

	/*
		Separate each TimeBucket from the result and compose a NumpyMultiDataset
	*/
	var nmds *io.NumpyMultiDataset
	for tbk, cs := range csm {
		nds, err := io.NewNumpyDataset(cs)
		if err != nil {
			return nil, err
		}
		if nmds == nil {
			nmds, err = io.NewNumpyMultiDataset(nds, tbk)
			if err != nil {
				return nil, err
			}
		} else {
			nmds.Append(cs, tbk)
		}
	}

	return &QueryResponse{nmds}, nil
}

type ListSymbolsResponse struct {
	Results []string
}

type ListSymbolsRequest struct {
	// "symbol", or "tbk"
	Format string `msgpack:"format,omitempty"`
}

func (s *DataService) ListSymbols(r *http.Request, req *ListSymbolsRequest, response *ListSymbolsResponse) (err error) {
	if atomic.LoadUint32(&Queryable) == 0 {
		return errNotQueryable
	}

	// TBK format (e.g. ["AMZN/1Min/TICK", "AAPL/1Sec/OHLCV", ...])
	if req != nil && req.Format == "tbk" {
		response.Results = catalog.ListTimeBucketKeyNames(s.catalogDir)
		return nil
	}

	// Symbol format (e.g. ["AMZN", "AAPL", ...])
	symbols := s.catalogDir.GatherCategoriesAndItems()["Symbol"]
	response.Results = make([]string, len(symbols))
	cnt := 0
	for symbol := range symbols {
		response.Results[cnt] = symbol
		cnt++
	}
	return nil
}

/*
Utility functions
*/

type QueryService struct {
	catalogDir *catalog.Directory
}

func NewQueryService(catDir *catalog.Directory) *QueryService {
	return &QueryService{
		catalogDir: catDir,
	}
}

func (qs *QueryService) ExecuteQuery(tbk *io.TimeBucketKey, start, end time.Time, limitRecordCount int,
	limitFromStart bool, columns []string,
) (io.ColumnSeriesMap, error) {
	query := planner.NewQuery(qs.catalogDir)

	/*
		Alter timeframe inside key to ensure it matches a queryable TF
	*/

	tf := tbk.GetItemInCategory("Timeframe")
	cd, err := utils.CandleDurationFromString(tf)
	if err != nil {
		return nil, fmt.Errorf("timeframe not found in TimeBucketKey=%s: %w", tbk.String(), err)
	}
	queryableTimeframe := cd.QueryableTimeframe()
	tbk.SetItemInCategory("Timeframe", queryableTimeframe)
	query.AddTargetKey(tbk)

	if limitRecordCount != 0 {
		direction := io.LAST
		if limitFromStart {
			direction = io.FIRST
		}
		query.SetRowLimit(
			direction,
			cd.QueryableNrecords(
				queryableTimeframe,
				limitRecordCount,
			),
		)
	}

	query.SetRange(start, end)
	parseResult, err := query.Parse()
	if err != nil {
		// No results from query
		if err.Error() == "no files returned from query parse" {
			log.Info("no results returned from query: Target: %v, start, end: %v,%v limitRecordCount: %v",
				tbk.String(), start, end, limitRecordCount)
		} else {
			log.Error("Parsing query: %s\n", err)
		}
		return nil, err
	}
	scanner, err := executor.NewReader(parseResult)
	if err != nil {
		log.Error("Unable to create scanner: %s\n", err)
		return nil, err
	}
	csm, err := scanner.Read()
	if err != nil {
		log.Error("Error returned from query scanner: %s\n", err)
		return nil, err
	}

	csm.FilterColumns(columns)

	return csm, err
}
