package sqlparser

import (
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/candler/candlecandler"
	"github.com/alpacahq/marketstore/v4/contrib/candler/tickcandler"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
	"github.com/alpacahq/marketstore/v4/uda/avg"
	"github.com/alpacahq/marketstore/v4/uda/count"
	"github.com/alpacahq/marketstore/v4/uda/gap"
	"github.com/alpacahq/marketstore/v4/uda/max"
	"github.com/alpacahq/marketstore/v4/uda/min"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type AggRunner struct {
	registry map[string]uda.AggInterface
}

func NewAggRunner(registry map[string]uda.AggInterface) *AggRunner {
	if registry == nil {
		return &AggRunner{registry: map[string]uda.AggInterface{}}
	}
	return &AggRunner{registry: registry}
}

func NewDefaultAggRunner(catDir *catalog.Directory) *AggRunner {
	return NewAggRunner(
		map[string]uda.AggInterface{
			"tickcandler":   &tickcandler.TickCandler{},
			"candlecandler": &candlecandler.CandleCandler{},
			"count":         &count.Count{},
			"min":           &min.Min{},
			"max":           &max.Max{},
			"avg":           &avg.Avg{},
			"gap":           &gap.Gap{},
			"adjust":        &adjust.Adjust{CatalogDir: catDir},
		},
	)
}

func (ar *AggRunner) GetFunc(aggName string) uda.AggInterface {
	return ar.registry[strings.ToLower(aggName)]
}

func (ar *AggRunner) Run(callChain []string, csInput *io.ColumnSeries, tbk io.TimeBucketKey,
) (cs *io.ColumnSeries, err error) {
	cs = nil
	for _, call := range callChain {
		if cs != nil {
			csInput = cs
		}
		aggName, literalList, parameterList, err := ParseFunctionCall(call)
		if err != nil {
			return nil, err
		}

		agg := ar.registry[strings.ToLower(aggName)]
		if agg == nil {
			return nil, fmt.Errorf("No function in the UDA Registry named \"%s\"", aggName)
		}
		argMap := functions.NewArgumentMap(agg.GetRequiredArgs(), agg.GetOptionalArgs()...)
		if unmapped := argMap.Validate(); unmapped != nil {
			return nil, fmt.Errorf("unmapped columns: %s", unmapped)
		}

		err = argMap.PrepareArguments(parameterList)
		if err != nil {
			return nil, fmt.Errorf("Argument mapping error for %s: %s", aggName, err.Error())
		}

		/*
			Initialize the Aggregate
				An agg may have init parameters, which are used only to initialize it
				These are single value literals (like '1Min')
		*/
		requiredInitDSV := agg.GetInitArgs()
		requiredInitNames := io.GetNamesFromDSV(requiredInitDSV)

		if len(requiredInitNames) > len(literalList) {
			return nil, fmt.Errorf(
				"Not enough init arguments for %s, need %d have %d",
				aggName,
				len(requiredInitNames),
				len(literalList),
			)
		}
		aggfunc, err := agg.New(argMap, literalList)
		if err != nil {
			return nil, fmt.Errorf("init aggfunc during query: %w", err)
		}

		/*
			Execute the aggregate function
		*/
		if cs, err = aggfunc.Accum(tbk, argMap, csInput); err != nil {
			return nil, err
		}
		if cs == nil {
			return nil, fmt.Errorf(
				"No result from aggregate %s",
				aggName)
		}
	}
	return cs, nil
}

// ParseFunctionCall parses a string to call an aggregator function.
// e.g. "FuncName (P1, 'Lit1', P2,P3,P4, 'Lit2' , Sum::P5, Avg::P6)"
// -> funcName="FuncName" , literalList=["Lit1", "Lit2"], parameterList=["P1","P2","P3","P4","Sum::P5", "Avg::P6"].
func ParseFunctionCall(call string) (funcName string, literalList, parameterList []string, err error) {
	call = strings.Trim(call, " ")
	left := strings.Index(call, "(")
	right := strings.LastIndex(call, ")")
	if left == -1 || right == -1 {
		return "", nil, nil, fmt.Errorf("unable to parse function call %s", call)
	}
	funcName = strings.Trim(call[:left], " ")
	call = call[left+1 : right]
	/*
		First parse for literals and re-form a string without them for the last stage of parsing
	*/
	var newCall string
	for {
		left = strings.Index(call, "'")
		if left == -1 {
			newCall = newCall + call
			break
		} else if left != 0 {
			newCall = newCall + call[:left]
		}
		call = call[left+1:]
		right = strings.Index(call, "'")
		if right == -1 {
			return "", nil, nil, fmt.Errorf("unclosed literal %s", call)
		}
		literalList = append(literalList, call[:right])
		call = call[right+1:]
	}
	pList := strings.Split(newCall, ",")
	for _, val := range pList {
		trimmed := strings.Trim(val, " ")
		if len(trimmed) != 0 {
			parameterList = append(parameterList, trimmed)
		}
	}
	return funcName, literalList, parameterList, nil
}
