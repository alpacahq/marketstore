package count

import (
	"time"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

/*
This is filled in for example purposes, should be overridden in implementation.
*/
var requiredColumns = []io.DataShape{
	{Name: "*", Type: io.INT64},
}

/*
For the optional inputs, we'll postpend the input names mapped to each optional
for output, for example: if we map user input "Volume" to "Sum", the output
will be "Sum_Volume".
*/
var optionalColumns = []io.DataShape{}

var initArgs = []io.DataShape{}

type Count struct {
	uda.AggInterface

	Sum int64
}

func (c *Count) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (c *Count) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (c *Count) GetInitArgs() []io.DataShape {
	return initArgs
}

// Accum sends new data to the aggregate
func (c *Count) Accum(_ io.TimeBucketKey, _ *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	c.Sum += int64(cols.Len())
	return c.Output(), nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (c Count) New(argMap *functions.ArgumentMap, itf ...interface{}) (out uda.AggInterface, err error) {
	return &Count{
		Sum: 0,
	}, nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (c *Count) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Count", []int64{c.Sum})
	return cs
}
