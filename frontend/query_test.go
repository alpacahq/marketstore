package frontend

import (
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"

	"time"

	"fmt"

	"math"

	. "gopkg.in/check.v1"
)

func (s *ServerTestSuite) _TestQueryCustomTimeframes(c *C) {
	//TODO: Support custom timeframes
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY,EURUSD/30Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(10).
				LimitFromStart(false).
				End()),
			(NewQueryRequestBuilder("USDJPY,EURUSD/1W/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(10).
				LimitFromStart(false).
				End()),
			(NewQueryRequestBuilder("USDJPY/1M/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(5).
				LimitFromStart(false).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err.Error())
	}

	c.Check(len(response.Responses), Equals, 3)
	c.Check(len(response.Responses[0].Result.StartIndex), Equals, 2)
	c.Check(len(response.Responses[1].Result.StartIndex), Equals, 2)
	c.Check(len(response.Responses[2].Result.StartIndex), Equals, 1)
	csm, err := response.Responses[0].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	c.Assert(len(csm), Equals, 2)

	csm, err = response.Responses[1].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	c.Assert(len(csm), Equals, 2)

	csm, err = response.Responses[2].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	c.Assert(len(csm), Equals, 1)
}

func (s *ServerTestSuite) TestQuery(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(200).
				LimitFromStart(false).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	c.Assert(len(response.Responses[0].Result.ColumnNames), Equals, 5) // key + OHLC

	cs, err := response.Responses[0].Result.ToColumnSeries()
	c.Assert(err == nil, Equals, true)

	index := cs.GetEpoch()
	c.Assert(len(index), Equals, 200)
	lastTime := index[len(index)-1]
	t := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	c.Assert(t, Equals, tref)
}

func (s *ServerTestSuite) TestQueryFirstN(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(200).
				LimitFromStart(true).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	cs, err := response.Responses[0].Result.ToColumnSeries()
	c.Assert(err == nil, Equals, true)
	index := cs.GetEpoch()
	c.Assert(len(index), Equals, 200)

	firstTime := index[0]
	t := time.Unix(firstTime, 0).UTC()
	tref := test.ParseT("2000-01-01 00:00:00")
	c.Assert(t, Equals, tref)
}

func (s *ServerTestSuite) TestQueryRange(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()
	{
		args := &MultiQueryRequest{
			Requests: []QueryRequest{
				(NewQueryRequestBuilder("USDJPY/1Min/OHLC").
					EpochStart(time.Date(2002, time.October, 1, 10, 5, 0, 0, time.UTC).Unix()).
					EpochEnd(time.Date(2002, time.October, 1, 15, 5, 0, 0, time.UTC).Unix()).
					LimitRecordCount(0).
					LimitFromStart(false).
					End()),
			},
		}

		var response MultiQueryResponse
		if err := service.Query(nil, args, &response); err != nil {
			c.Fatalf("error returned: %s", err)
		}
		cs, _ := response.Responses[0].Result.ToColumnSeries()
		index := cs.GetEpoch()
		c.Logf("EPOCH: %v", index)
		c.Assert(time.Unix(index[0], 0), Equals, time.Unix(*args.Requests[0].EpochStart, 0))
	}

	{
		args := &MultiQueryRequest{
			Requests: []QueryRequest{
				(NewQueryRequestBuilder("USDJPY/5Min/OHLC").
					EpochStart(test.ParseT("2002-12-31 00:00:00").Unix()).
					EpochEnd(math.MaxInt32).
					LimitRecordCount(0).
					LimitFromStart(false).
					End()),
			},
		}

		var response MultiQueryResponse
		if err := service.Query(nil, args, &response); err != nil {
			c.Fatalf("error returned: %s", err)
		}
		cs, _ := response.Responses[0].Result.ToColumnSeries()
		index := cs.GetEpoch()
		c.Assert(len(index), Equals, 288)
	}
}

func (s *ServerTestSuite) TestQueryNpyMulti(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY,EURUSD/1Min/OHLC").
				LimitRecordCount(200).
				LimitFromStart(false).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	c.Check(len(response.Responses[0].Result.StartIndex), Equals, 2)
	csm, err := response.Responses[0].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		c.Fail()
	}
	for _, cs := range csm {
		c.Assert(cs.Len() == 200, Equals, true)
	}
	c.Assert(len(csm), Equals, 2)
}

func (s *ServerTestSuite) TestQueryMulti(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY,EURUSD/1Min/OHLC").
				LimitRecordCount(200).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	csm, _ := response.Responses[0].Result.ToColumnSeriesMap()

	tbk := io.NewTimeBucketKey("USDJPY/1Min/OHLC")
	usdjpy := csm[*tbk]
	usdjpy_index := usdjpy.GetEpoch()
	tbk = io.NewTimeBucketKey("EURUSD/1Min/OHLC")
	eurusd := csm[*tbk]
	eurusd_index := eurusd.GetEpoch()

	c.Assert(len(usdjpy.GetColumnNames()), Equals, 5) // key + OHLC
	c.Assert(len(usdjpy_index), Equals, 200)
	lastTime := usdjpy_index[len(usdjpy_index)-1]
	t := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	c.Assert(t, Equals, tref)

	c.Assert(len(eurusd.GetColumnNames()), Equals, 5) // key + OHLC + prev
	c.Assert(len(eurusd_index), Equals, 200)
	lastTime = eurusd_index[len(eurusd_index)-1]
	t = time.Unix(lastTime, 0).UTC()
	tref = time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	c.Assert(t, Equals, tref)
}

func (s *ServerTestSuite) TestListSymbols(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	var response ListSymbolsResponse
	if err := service.ListSymbols(nil, nil, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	contains := func(s []string, e string) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	c.Assert(contains(response.Results, "EURUSD"), Equals, true)
	c.Assert(contains(response.Results, "USDJPY"), Equals, true)

	var resp ListSymbolsResponse

	req := &ListSymbolsRequest{}

	if err := service.ListSymbols(nil, req, &resp); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	c.Assert(contains(resp.Results, "EURUSD"), Equals, true)
	c.Assert(contains(resp.Results, "USDJPY"), Equals, true)
}

func (s *ServerTestSuite) TestFunctions(c *C) {
	service := NewDataService(false, false, s.Rootdir)
	service.Init()

	call := "candlecandler('1Min',Open,High,Low,Close,Sum::Volume)"
	fname, l_list, p_list, err := parseFunctionCall(call)
	if err != nil {
		fmt.Println(err)
		c.FailNow()
	}
	//	printFuncParams(fname, l_list, p_list)

	call = "FuncName (P1, 'Lit1', P2,P3,P4, 'Lit2' , Sum::P5, Avg::P6)"
	fname, l_list, p_list, err = parseFunctionCall(call)
	if err != nil {
		fmt.Println(err)
		c.FailNow()
	}
	//	printFuncParams(fname, l_list, p_list)
	c.Assert(fname, Equals, "FuncName")
	c.Assert(l_list[0], Equals, "Lit1")
	c.Assert(l_list[1], Equals, "Lit2")
	c.Assert(p_list[0], Equals, "P1")
	c.Assert(p_list[1], Equals, "P2")
	c.Assert(p_list[2], Equals, "P3")
	c.Assert(p_list[3], Equals, "P4")
	c.Assert(p_list[4], Equals, "Sum::P5")
	c.Assert(p_list[5], Equals, "Avg::P6")

	args := &MultiQueryRequest{
		Requests: []QueryRequest{
			(NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				LimitRecordCount(200).
				Functions([]string{"candlecandler('5Min',Open,High,Low,Close)"}).
				End()),
		},
	}

	var response MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err)
	}

	c.Assert(len(response.Responses[0].Result.ColumnNames), Equals, 5) // key + OHLC

	cs, err := response.Responses[0].Result.ToColumnSeries()
	c.Assert(err == nil, Equals, true)

	index := cs.GetEpoch()
	c.Assert(len(index), Equals, 40)
	lastTime := index[len(index)-1]
	t := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 55, 0, 0, time.UTC)
	c.Assert(t, Equals, tref)
}

func printFuncParams(fname string, l_list, p_list []string) {
	fmt.Printf("LAL funcName=:%s:\n", fname)
	for i, val := range l_list {
		fmt.Printf("LAL literal[%d]=:%s:\n", i, val)
	}
	for i, val := range p_list {
		fmt.Printf("LAL param[%d]=:%s:\n", i, val)
	}
}
