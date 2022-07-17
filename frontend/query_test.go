package frontend_test

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T,
) (rootDir string, metadata *executor.InstanceMetadata, writer *executor.Writer,
	q frontend.QueryInterface,
) {
	t.Helper()

	rootDir = t.TempDir()
	test.MakeDummyCurrencyDir(rootDir, true, false)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	metadata = executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	atomic.StoreUint32(&frontend.Queryable, uint32(1))

	qs := frontend.NewQueryService(c.GetCatalogDir())
	writer, _ = executor.NewWriter(c.GetCatalogDir(), c.GetInitWALFile())
	return rootDir, metadata, writer, qs
}

func TestQueryCustomTimeframes(t *testing.T) {
	// TODO: Support custom timeframes

	// rootDir, metadata, writer, q := setup(t)

	//service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	//service.Init()
	//
	//args := &frontend.MultiQueryRequest{
	//	Requests: []frontend.QueryRequest{
	//		frontend.NewQueryRequestBuilder("USDJPY,EURUSD/30Min/OHLC").
	//			EpochStart(0).
	//			EpochEnd(math.MaxInt32).
	//			LimitRecordCount(10).
	//			LimitFromStart(false).
	//			End(),
	//		frontend.NewQueryRequestBuilder("USDJPY,EURUSD/1W/OHLC").
	//			EpochStart(0).
	//			EpochEnd(math.MaxInt32).
	//			LimitRecordCount(10).
	//			LimitFromStart(false).
	//			End(),
	//		frontend.NewQueryRequestBuilder("USDJPY/1M/OHLC").
	//			EpochStart(0).
	//			EpochEnd(math.MaxInt32).
	//			LimitRecordCount(5).
	//			LimitFromStart(false).
	//			End(),
	//	},
	//}
	//
	//var response frontend.MultiQueryResponse
	//if err := service.Query(nil, args, &response); err != nil {
	//	t.Fatalf("error returned: %s", err.Error())
	//}
	//
	//assert.Len(t, response.Responses, 3)
	//assert.Len(t, response.Responses[0].Result.StartIndex, 2)
	//assert.Len(t, response.Responses[1].Result.StartIndex, 2)
	//assert.Len(t, response.Responses[2].Result.StartIndex, 1)
	//csm, err := response.Responses[0].Result.ToColumnSeriesMap()
	//if err != nil {
	//	t.Log(err)
	//	t.Fail()
	//}
	//assert.Len(t, csm, 2)
	//
	//csm, err = response.Responses[1].Result.ToColumnSeriesMap()
	//if err != nil {
	//	t.Log(err)
	//	t.Fail()
	//}
	//assert.Len(t, csm, 2)
	//
	//csm, err = response.Responses[2].Result.ToColumnSeriesMap()
	//if err != nil {
	//	t.Log(err)
	//	t.Fail()
	//}
	//assert.Len(t, csm, 1)
}

func TestQuery(t *testing.T) {
	rootDir, metadata, writer, q := setup(t)

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
	rootDir, metadata, writer, q := setup(t)

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
	rootDir, metadata, writer, q := setup(t)

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
	rootDir, metadata, writer, q := setup(t)

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
		t.Log(err)
		t.Fail()
	}
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 200)
	}
	assert.Len(t, csm, 2)
}

func TestQueryMulti(t *testing.T) {
	rootDir, metadata, writer, q := setup(t)

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
	usdjpyIndex := usdjpy.GetEpoch()
	tbk = io.NewTimeBucketKey("EURUSD/1Min/OHLC")
	eurusd := csm[*tbk]
	eurusdIndex := eurusd.GetEpoch()

	assert.Len(t, usdjpy.GetColumnNames(), 5) // key + OHLC
	assert.Len(t, usdjpyIndex, 200)
	lastTime := usdjpyIndex[len(usdjpyIndex)-1]
	ti := time.Unix(lastTime, 0).UTC()
	tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)

	assert.Len(t, eurusd.GetColumnNames(), 5) // key + OHLC + prev
	assert.Len(t, eurusdIndex, 200)
	lastTime = eurusdIndex[len(eurusdIndex)-1]
	ti = time.Unix(lastTime, 0).UTC()
	tref = time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
	assert.Equal(t, ti, tref)
}

func TestListSymbols(t *testing.T) {
	rootDir, metadata, writer, q := setup(t)

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
	rootDir, metadata, writer, q := setup(t)

	service := frontend.NewDataService(rootDir, metadata.CatalogDir,
		sqlparser.NewDefaultAggRunner(metadata.CatalogDir), writer, q,
	)
	service.Init()

	call := "candlecandler('1Min',Open,High,Low,Close,Sum::Volume)"
	_, _, _, err := sqlparser.ParseFunctionCall(call)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	//	printFuncParams(fname, lList, pList)

	call = "FuncName (P1, 'Lit1', P2,P3,P4, 'Lit2' , Sum::P5, Avg::P6)"
	fname, lList, pList, err := sqlparser.ParseFunctionCall(call)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	//	printFuncParams(fname, lList, pList)
	assert.Equal(t, fname, "FuncName")
	assert.Equal(t, lList[0], "Lit1")
	assert.Equal(t, lList[1], "Lit2")
	assert.Equal(t, pList[0], "P1")
	assert.Equal(t, pList[1], "P2")
	assert.Equal(t, pList[2], "P3")
	assert.Equal(t, pList[3], "P4")
	assert.Equal(t, pList[4], "Sum::P5")
	assert.Equal(t, pList[5], "Avg::P6")

	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY/1Min/OHLC").
				LimitRecordCount(200).
				Functions([]string{"candlecandler('5Min',Open,High,Low,Close)"}).
				End(),
		},
	}

	var response frontend.MultiQueryResponse
	if err2 := service.Query(nil, args, &response); err2 != nil {
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
