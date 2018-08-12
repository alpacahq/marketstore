package session

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/SQLParser"
	"github.com/alpacahq/marketstore/frontend"
	"github.com/alpacahq/marketstore/utils/io"
)

// sql executes a sql statement against the current db.
func (c *Client) sql(line string) {
	timeStart := time.Now()
	var err error
	var cs *io.ColumnSeries
	if c.mode == local {
		cs, err = localSQL(line)
	} else {
		cs, err = c.remoteSQL(line)
	}

	if err != nil {
		fmt.Println(err)
		return
	}

	runTime := time.Since(timeStart)

	err = printResult(line, cs)
	if err != nil {
		fmt.Println(err.Error())
	}

	if c.timing {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*runTime.Seconds())
	}
}

func localSQL(line string) (cs *io.ColumnSeries, err error) {
	ast, err := SQLParser.NewAstBuilder(line)
	if err != nil {
		return nil, err
	}
	es, err := SQLParser.NewExecutableStatement(ast.Mtree)
	if err != nil {
		return nil, err
	}
	cs, err = es.Materialize()
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func (c *Client) remoteSQL(line string) (cs *io.ColumnSeries, err error) {
	req := frontend.QueryRequest{
		IsSQLStatement: true,
		SQLStatement:   line,
	}
	args := &frontend.MultiQueryRequest{Requests: []frontend.QueryRequest{req}}

	resp, err := c.rc.DoRPC("Query", args)
	if err != nil {
		return nil, err
	}

	for _, sub := range *resp.(*io.ColumnSeriesMap) {
		cs = sub
		break
	}
	return cs, err
}
