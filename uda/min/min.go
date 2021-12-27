package min

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

type Min struct {
	uda.AggInterface

	IsInitialized bool
	Min           float32
}

func (mn *Min) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (mn *Min) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (mn *Min) GetInitArgs() []io.DataShape {
	return initArgs
}

/*
	Accum() sends new data to the aggregate
*/
func (mn *Min) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	if cols.Len() == 0 {
		return mn.Output(), nil
	}
	inputColDSV := argMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		return nil, err
	}

	if !mn.IsInitialized {
		mn.Min = inputCol[0]
		mn.IsInitialized = true
	}
	for _, value := range inputCol {
		if value < mn.Min {
			mn.Min = value
		}
	}
	return mn.Output(), nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (m Min) New(argMap *functions.ArgumentMap, itf ...interface{}) (out uda.AggInterface, err error) {
	mn := &Min{
		IsInitialized: false,
		Min:           0,
	}

	if unmapped := argMap.Validate(); unmapped != nil {
		return nil, fmt.Errorf("Unmapped columns: %s", unmapped)
	}

	return mn, nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (mn *Min) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Min", []float32{mn.Min})
	return cs
}
