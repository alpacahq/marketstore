package avg

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	requiredColumns = []io.DataShape{
		{Name: "*", Type: io.FLOAT32},
	}

	optionalColumns = []io.DataShape{}

	initArgs = []io.DataShape{}
)

type Avg struct {
	uda.AggInterface

	Avg   float64
	Count int64
}

func (a *Avg) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (a *Avg) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (a *Avg) GetInitArgs() []io.DataShape {
	return initArgs
}

// Accum sends new data to the aggregate
func (a *Avg) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	if cols.Len() == 0 {
		return a.Output(), nil
	}
	inputColDSV := argMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		fmt.Println("COLS: ", cols)
		return nil, err
	}

	for _, value := range inputCol {
		a.Avg += float64(value)
		a.Count++
	}
	return a.Output(), nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (a Avg) New(_ *functions.ArgumentMap, _ ...interface{}) (out uda.AggInterface, err error) {
	return &Avg{
		Avg:   0,
		Count: 0,
	}, nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (a *Avg) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Avg", []float64{a.Avg / float64(a.Count)})
	return cs
}
