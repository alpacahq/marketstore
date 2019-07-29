package sqlparser

import (
	"encoding/json"

	"github.com/alpacahq/marketstore/utils/io"
)

type ExplainStatement struct {
	ExecutableStatement
	QueryText string
}

func NewExplainStatement(ctx *StatementParse, queryText string) (es *ExplainStatement) {
	es = new(ExplainStatement)
	es.QueryText = queryText
	es.AddChild(ctx)
	return es
}

func (es *ExplainStatement) Materialize() (cs *io.ColumnSeries, err error) {
	result := Explain(es.GetChild(0))
	cs = io.NewColumnSeries()
	cs.AddColumn("explain-output", result)
	return cs, nil
}

func (es *ExplainStatement) Explain() string {
	if es != nil {
		jsonStruct, _ := json.Marshal(*es)
		return string(jsonStruct)
	} else {
		return "{}"
	}
}

func (es *ExplainStatement) GetLeft() IMSTree {
	if es.GetChildCount() == 0 {
		return nil
	} else {
		return es.GetChild(0)
	}
}

func (es *ExplainStatement) GetRight() IMSTree {
	if es.GetChildCount() < 2 {
		return nil
	} else {
		return es.GetChild(1)
	}
}

/*
Utility Structures
*/
