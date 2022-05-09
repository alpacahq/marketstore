package frontend_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestWrite(t *testing.T) {
	rootDir, metadata, writer, q := setup(t)

	service := frontend.NewDataService(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	service.Init()

	qargs := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("USDJPY,EURUSD/1Min/OHLC").
				LimitRecordCount(201).
				End(),
		},
	}

	var qresponse frontend.MultiQueryResponse
	if err := service.Query(nil, qargs, &qresponse); err != nil {
		t.Fatalf("error returned: %s", err.Error())
	}

	/*
		Alter the destinations inside the NumpyDataSet and re-write
	*/
	nmds := qresponse.Responses[0].Result
	var i int
	si := make(map[string]int)
	li := make(map[string]int)
	for tbkStr, val := range nmds.StartIndex {
		tbk := io.NewTimeBucketKeyFromString(tbkStr)
		tbk.SetItemInCategory("Symbol", "TEST"+strconv.Itoa(i))
		si[tbk.String()] = val
		li[tbk.String()] = nmds.Lengths[tbkStr]
		i++
	}
	nmds.StartIndex = si
	nmds.Lengths = li

	csm2, _ := qresponse.Responses[0].Result.ToColumnSeriesMap()
	for _, cs := range csm2 {
		//	t.Log("LAL cs len:", cs.Len())
		assert.Equal(t, cs.Len(), 201)
	}

	args := &frontend.MultiWriteRequest{
		Requests: []frontend.WriteRequest{
			{
				Data:             qresponse.Responses[0].Result,
				IsVariableLength: false,
			},
		},
	}

	var response frontend.MultiServerResponse
	if err := service.Write(nil, args, &response); err != nil {
		t.Fatalf("error returned: %s", err.Error())
	}

	for _, resp := range response.Responses {
		if resp.Error != "" {
			t.Logf("Error: %s\n", resp.Error)
			t.FailNow()
		}
	}

	/*
		Read the newly written data back and verify
	*/
	qargs = &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{
			frontend.NewQueryRequestBuilder("TEST0,TEST1/1Min/OHLC").
				LimitRecordCount(200).
				End(),
		},
	}

	qresponse = frontend.MultiQueryResponse{}
	if err := service.Query(nil, qargs, &qresponse); err != nil {
		t.Fatalf("error returned: %s", err.Error())
	}
	csm, err := qresponse.Responses[0].Result.ToColumnSeriesMap()
	assert.Nil(t, err)

	for _, cs := range csm {
		index := cs.GetEpoch()
		assert.Len(t, index, 200)
		lastTime := index[len(index)-1]
		ti := time.Unix(lastTime, 0).UTC()
		tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
		assert.Equal(t, ti, tref)
	}
}
