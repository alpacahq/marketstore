package uda

import (
	"time"

	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

/*
An aggregate is a function that takes rows as input and outputs a processed set
of rows

The contract with the agg function is: the input rows must conform to the
expected inputs of the agg, known to the caller. For example: If we have an agg
that outputs "candles", there must be a column in the input rows that can be
used to evaluate "Price". This must be a named column in the input. The output
of the candler aggregate will always feature "Open, High, Low, Close", but may
also feature summed values like "Volume" or averaged values like VWAP (volume
weighted average price).

An agg has a set of "must have" input columns and can output a differing number
of columns (and rows) depending on inputs.
*/
type AggInterface interface {
	FunctionInterface
	/*
		Input arguments, followed by a custom set of arguments
	*/
	New(argMap *functions.ArgumentMap, args ...interface{}) (AggInterface, error)

	/*
		Accum() sends new data to the aggregate
		and returns the currently valid output of this aggregate
	*/
	//Accum(ts []time.Time, rows io.Rows)
	// The io.ColumnInterface parameter is one of; ColumnSeries or Rows
	Accum(io.TimeBucketKey, *functions.ArgumentMap, io.ColumnInterface) (*io.ColumnSeries, error)
}

//TODO: This is where we break out a UDF API.
type FunctionInterface interface {
	GetRequiredArgs() []io.DataShape
	GetOptionalArgs() []io.DataShape
	GetInitArgs() []io.DataShape
}

/*
Utility Datatypes
*/

/*
Sortable time slice.
*/
type OrderedTime []time.Time

func (ot OrderedTime) Len() int           { return len(ot) }
func (ot OrderedTime) Swap(i, j int)      { ot[i], ot[j] = ot[j], ot[i] }
func (ot OrderedTime) Less(i, j int) bool { return ot[i].Before(ot[j]) }
