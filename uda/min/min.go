package min

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/functions"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/uda"
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
func (mn *Min) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface, _ *catalog.Directory) error {
	if cols.Len() == 0 {
		return nil
	}
	inputColDSV := argMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		return err
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
	return nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (m Min) New() (out uda.AggInterface) {
	mn := NewCount()
	return mn
}

/*
CONCRETE - these may be suitable methods for general usage
*/
func NewCount() (mn *Min) {
	mn = new(Min)
	return mn
}
func (mn *Min) Init(argMap *functions.ArgumentMap, itf ...interface{}) error {
	if unmapped := argMap.Validate(); unmapped != nil {
		return fmt.Errorf("Unmapped columns: %s", unmapped)
	}
	mn.Min = 0
	mn.IsInitialized = false
	return nil
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
