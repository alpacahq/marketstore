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

func (m *Min) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (m *Min) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (m *Min) GetInitArgs() []io.DataShape {
	return initArgs
}

// Accum sends new data to the aggregate
func (m *Min) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	if cols.Len() == 0 {
		return m.Output(), nil
	}
	inputColDSV := argMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		return nil, err
	}

	if !m.IsInitialized {
		m.Min = inputCol[0]
		m.IsInitialized = true
	}
	for _, value := range inputCol {
		if value < m.Min {
			m.Min = value
		}
	}
	return m.Output(), nil
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
		return nil, fmt.Errorf("unmapped columns: %s", unmapped)
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
