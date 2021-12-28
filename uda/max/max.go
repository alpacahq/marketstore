package max

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

type Max struct {
	uda.AggInterface

	IsInitialized bool
	Max           float32
}

func (ma *Max) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (ma *Max) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (ma *Max) GetInitArgs() []io.DataShape {
	return initArgs
}

// Accum sends new data to the aggregate
func (ma *Max) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	if cols.Len() == 0 {
		return ma.Output(), nil
	}
	inputColDSV := argMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		return nil, err
	}

	if !ma.IsInitialized {
		ma.Max = inputCol[0]
		ma.IsInitialized = true
	}
	for _, value := range inputCol {
		if value > ma.Max {
			ma.Max = value
		}
	}
	return ma.Output(), nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (m Max) New(argMap *functions.ArgumentMap, itf ...interface{}) (out uda.AggInterface, err error) {
	if unmapped := argMap.Validate(); unmapped != nil {
		return nil, fmt.Errorf("unmapped columns: %s", unmapped)
	}

	return &Max{
		IsInitialized: false,
		Max:           0,
	}, nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (ma *Max) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Max", []float32{ma.Max})
	return cs
}
