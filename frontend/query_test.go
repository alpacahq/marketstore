package frontend_test

import (
	"fmt"
	"io/ioutil"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/sqlparser"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T, testName string,
) (tearDown func(), rootDir string, metadata *executor.InstanceMetadata, writer *executor.Writer,
	q frontend.QueryInterface,
) {
	t.Helper()

	rootDir, _ = ioutil.TempDir("", fmt.Sprintf("frontend_test-%s", testName))
	test.MakeDummyCurrencyDir(rootDir, true, false)
	metadata, _, _ = executor.NewInstanceSetup(rootDir, nil, nil, 5, true, true, false)
	atomic.StoreUint32(&frontend.Queryable, uint32(1))

	qs := frontend.NewQueryService(metadata.CatalogDir)
	writer, _ = executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	return func() { test.CleanupDummyDataDir(rootDir) }, rootDir, metadata, writer, qs
}

func _TestQueryCustomTimeframes(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "_TestQueryCustomTimeframes")
	defer tearDown()

	//TODO: Support custom timeframes
	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY,EURUSD/30Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(10).
				LimitFromStart(false).
				End(),
			frontend.NewQueryRequestBuilder("USDJPY,EURUSD/1W/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(10).
				LimitFromStart(false).
				End(),
			frontend.NewQueryRequestBuilder("USDJPY/1M/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(5).
				LimitFromStart(false).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err.Error())
	}

	assert.Len(t, response.Responses, 3)
	assert.Len(t, response.Responses[0].Result.StartIndex, 2)
	assert.Len(t, response.Responses[1].Result.StartIndex, 2)
	assert.Len(t, response.Responses[2].Result.StartIndex, 1)
	csm, err := response.Responses[0].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	assert.Len(t, csm, 2)

	csm, err = response.Responses[1].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	assert.Len(t, csm, 2)

	csm, err = response.Responses[2].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	assert.Len(t, csm, 1)
}

func TestQuery(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestQuery")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(200).
				LimitFromStart(false).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	assert.Len(t, response.Responses[0].Result.ColumnNames, 5) // key + OHLC

	cs, err := response.Responses[0].Result.ToColumnSeries()
	assert.Nil(t, err)

	index := cs.GetEpoch()
	assert.Len(t, index, 200)
	lastTime := index[len(index)-1]
	ti := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)
}

func TestQueryFirstN(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestQueryFirstN")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				EpochStart(0).
				EpochEnd(math.MaxInt32).
				LimitRecordCount(200).
				LimitFromStart(true).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	cs, err := response.Responses[0].Result.ToColumnSeries()
	assert.Nil(t, err)
	index := cs.GetEpoch()
	assert.Len(t, index, 200)

	firstTime := index[0]
	ti := time.Unix(firstTime, 0).UTC()
	tref := test.ParseT("2000-01-01 00:00:00")
	assert.Equal(t, ti, tref)
}

func TestQueryRange(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestQueryRange")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()
	{
		args := &frontend.MultiQueryRequest{
			Requests: []frontend.QueryRequest{
				frontend.NewQueryRequestBuilder("USDJPY/1Min/OHLC").
					EpochStart(time.Date(2002, time.October, 1, 10, 5, 0, 0, time.UTC).Unix()).
					EpochEnd(time.Date(2002, time.October, 1, 15, 5, 0, 0, time.UTC).Unix()).
					LimitRecordCount(0).
					LimitFromStart(false).
					End(),
			},
		}

		var response frontend.MultiQueryResponse
		if err := service.Query(nil, args, &response); err != nil {
			t.Fatalf("error returned: %s", err)
		}
		cs, _ := response.Responses[0].Result.ToColumnSeries()
		index := cs.GetEpoch()
		t.Logf("EPOCH: %v", index)
		assert.Equal(t, time.Unix(index[0], 0), time.Unix(*args.Requests[0].EpochStart, 0))
	}

	{
		args := &frontend.MultiQueryRequest{
			Requests: []frontend.QueryRequest{
				frontend.NewQueryRequestBuilder("USDJPY/5Min/OHLC").
					EpochStart(test.ParseT("2002-12-31 00:00:00").Unix()).
					EpochEnd(math.MaxInt32).
					LimitRecordCount(0).
					LimitFromStart(false).
					End(),
			},
		}

		var response frontend.MultiQueryResponse
		if err := service.Query(nil, args, &response); err != nil {
			t.Fatalf("error returned: %s", err)
		}
		cs, _ := response.Responses[0].Result.ToColumnSeries()
		index := cs.GetEpoch()
		assert.Len(t, index, 288)
	}
}

func TestQueryNpyMulti(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestQueryNpyMulti")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY,EURUSD/1Min/OHLC").
				LimitRecordCount(200).
				LimitFromStart(false).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	assert.Len(t, response.Responses[0].Result.StartIndex, 2)
	csm, err := response.Responses[0].Result.ToColumnSeriesMap()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 200)
	}
	assert.Len(t, csm, 2)
}

func TestQueryMulti(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestQueryMulti")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY,EURUSD/1Min/OHLC").
				LimitRecordCount(200).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	csm, _ := response.Responses[0].Result.ToColumnSeriesMap()

	tbk := io.NewTimeBucketKey("USDJPY/1Min/OHLC")
	usdjpy := csm[*tbk]
	usdjpy_index := usdjpy.GetEpoch()
	tbk = io.NewTimeBucketKey("EURUSD/1Min/OHLC")
	eurusd := csm[*tbk]
	eurusd_index := eurusd.GetEpoch()

	assert.Len(t, usdjpy.GetColumnNames(), 5) // key + OHLC
	assert.Len(t, usdjpy_index, 200)
	lastTime := usdjpy_index[len(usdjpy_index)-1]
	ti := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)

	assert.Len(t, eurusd.GetColumnNames(), 5) // key + OHLC + prev
	assert.Len(t, eurusd_index, 200)
	lastTime = eurusd_index[len(eurusd_index)-1]
	ti = time.Unix(lastTime, 0).UTC()
	tref = time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)
}

func TestListSymbols(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestListSymbols")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	var response frontend.ListSymbolsResponse
	if err := service.ListSymbols(nil, nil, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	contains := func(s []string, e string) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	assert.True(t, contains(response.Results, "EURUSD"))
	assert.True(t, contains(response.Results, "USDJPY"))

	var resp frontend.ListSymbolsResponse

	req := &frontend.ListSymbolsRequest{}

	if err := service.ListSymbols(nil, req, &resp); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	assert.True(t, contains(resp.Results, "EURUSD"))
	assert.True(t, contains(resp.Results, "USDJPY"))
}

func TestFunctions(t *testing.T) {
	tearDown, rootDir, metadata, writer, q := setup(t, "TestFunctions")
	defer tearDown()

	service := frontend.NewDataService(rootDir, metadata.CatalogDir,
		sqlparser.NewDefaultAggRunner(metadata.CatalogDir), writer, q,
	)
	service.Init()

	call := "candlecandler('1Min',Open,High,Low,Close,Sum::Volume)"
	_, _, p_list, err := sqlparser.ParseFunctionCall(call)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	//	printFuncParams(fname, l_list, p_list)

	call = "FuncName (P1, 'Lit1', P2,P3,P4, 'Lit2' , Sum::P5, Avg::P6)"
	fname, l_list, p_list, err := sqlparser.ParseFunctionCall(call)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	//	printFuncParams(fname, l_list, p_list)
	assert.Equal(t, fname, "FuncName")
	assert.Equal(t, l_list[0], "Lit1")
	assert.Equal(t, l_list[1], "Lit2")
	assert.Equal(t, p_list[0], "P1")
	assert.Equal(t, p_list[1], "P2")
	assert.Equal(t, p_list[2], "P3")
	assert.Equal(t, p_list[3], "P4")
	assert.Equal(t, p_list[4], "Sum::P5")
	assert.Equal(t, p_list[5], "Avg::P6")

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				LimitRecordCount(200).
				Functions([]string{"candlecandler('5Min',Open,High,Low,Close)"}).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err := service.Query(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err)
	}

	assert.Len(t, response.Responses[0].Result.ColumnNames, 5) // key + OHLC

	cs, err := response.Responses[0].Result.ToColumnSeries()
	assert.Nil(t, err)

	index := cs.GetEpoch()
	assert.Len(t, index, 40)
	lastTime := index[len(index)-1]
	ti := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 55, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)
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
