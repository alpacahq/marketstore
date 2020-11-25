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

	// Input arguments mapping
	ArgMap *functions.ArgumentMap

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
func (mn *Min) Accum(cols io.ColumnInterface) error {
	if cols.Len() == 0 {
		return nil
	}
	inputColDSV := mn.ArgMap.GetMappedColumns(requiredColumns[0].Name)
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
func (m Min) New() (out uda.AggInterface, am *functions.ArgumentMap) {
	mn := NewCount(requiredColumns, optionalColumns)
	return mn, mn.ArgMap
}

/*
CONCRETE - these may be suitable methods for general usage
*/
func NewCount(inputColumns, optionalInputColumns []io.DataShape) (mn *Min) {
	mn = new(Min)
	mn.ArgMap = functions.NewArgumentMap(inputColumns, optionalInputColumns...)
	return mn
}
func (mn *Min) Init(itf ...interface{}) error {
	if unmapped := mn.ArgMap.Validate(); unmapped != nil {
		return fmt.Errorf("Unmapped columns: %s", unmapped)
	}
	mn.Reset()
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

/*
	Reset() puts the aggregate state back to "new"
*/
func (mn *Min) Reset() {
	mn.Min = 0
	mn.IsInitialized = false
}

/*
Utility Functions
*/

/*
	SetTimeBucketKey()
*/
func (mn *Min) SetTimeBucketKey(tbk io.TimeBucketKey) {
}
