package frontend

import (
	"github.com/alpacahq/marketstore/utils/io"

	"math"

	"fmt"

	"strconv"

	"time"

	. "gopkg.in/check.v1"
)

func (s *ServerTestSuite) TestWrite(c *C) {
	service := &DataService{}
	service.Init()

	qargs := &MultiQueryRequest{
		Requests: []QueryRequest{
			QueryRequest{
				IsSQLStatement: false,
				SQLStatement:   "",
				Destination:    *io.NewTimeBucketKey("USDJPY,EURUSD/1Min/OHLC"),
				TimeStart:      0, TimeEnd: math.MaxInt32,
				LimitRecordCount:   201,
				TimeOrderAscending: false,
			},
		},
	}

	var qresponse MultiQueryResponse
	if err := service.Query(nil, qargs, &qresponse); err != nil {
		c.Fatalf("error returned: %s", err.Error())
	}

	/*
		Alter the destinations inside the NumpyDataSet and re-write
	*/
	nmds := qresponse.Responses[0].Result
	var i int
	si := make(map[string]int)
	li := make(map[string]int)
	for tbkStr, val := range nmds.StartIndex {
		tbk, err := io.NewTimeBucketKeyFromString(tbkStr)
		if err != nil {
			fmt.Println(err)
			c.Fail()
		}
		tbk.SetItemInCategory("Symbol", "TEST"+strconv.Itoa(i))
		si[tbk.String()] = val
		li[tbk.String()] = nmds.Lengths[tbkStr]
		i++
	}
	nmds.StartIndex = si
	nmds.Lengths = li

	csm2, _ := qresponse.Responses[0].Result.ToColumnSeriesMap()
	for _, cs := range csm2 {
		//		fmt.Println("LAL cs len:", cs.Len())
		c.Assert(cs.Len(), Equals, 201)
	}

	args := &MultiWriteRequest{
		Requests: []WriteRequest{
			WriteRequest{
				Data:             qresponse.Responses[0].Result,
				IsVariableLength: false,
			},
		},
	}

	var response MultiWriteResponse
	if err := service.Write(nil, args, &response); err != nil {
		c.Fatalf("error returned: %s", err.Error())
	}

	for _, resp := range response.Responses {
		if len(resp.Error) != 0 {
			fmt.Printf("Error: %s\n", resp.Error)
			c.FailNow()
		}
	}

	/*
		Read the newly written data back and verify
	*/
	qargs = &MultiQueryRequest{
		Requests: []QueryRequest{
			QueryRequest{
				IsSQLStatement: false,
				SQLStatement:   "",
				Destination:    *io.NewTimeBucketKey("TEST0,TEST1/1Min/OHLC"),
				TimeStart:      0, TimeEnd: math.MaxInt32,
				LimitRecordCount:   200,
				TimeOrderAscending: false,
			},
		},
	}

	qresponse = MultiQueryResponse{}
	if err := service.Query(nil, qargs, &qresponse); err != nil {
		c.Fatalf("error returned: %s", err.Error())
	}
	csm, err := qresponse.Responses[0].Result.ToColumnSeriesMap()
	c.Assert(err == nil, Equals, true)

	for _, cs := range csm {
		index := cs.GetEpoch()
		c.Assert(len(index), Equals, 200)
		lastTime := index[len(index)-1]
		t := time.Unix(lastTime, 0).UTC()
		tref := time.Date(2002, time.December, 31, 23, 59, 0, 0, time.UTC)
		c.Assert(t, Equals, tref)
	}

}
