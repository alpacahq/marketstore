package avg

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
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

	// Input arguments mapping
	ArgMap *functions.ArgumentMap

	Avg   float64
	Count int64
}

func (av *Avg) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (av *Avg) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (av *Avg) GetInitArgs() []io.DataShape {
	return initArgs
}

/*
	Accum() sends new data to the aggregate
*/
func (av *Avg) Accum(_ io.TimeBucketKey, cols io.ColumnInterface, _ *catalog.Directory) error {
	if cols.Len() == 0 {
		return nil
	}
	inputColDSV := av.ArgMap.GetMappedColumns(requiredColumns[0].Name)
	inputColName := inputColDSV[0].Name
	inputCol, err := uda.ColumnToFloat32(cols, inputColName)
	if err != nil {
		fmt.Println("COLS: ", cols)
		return err
	}

	for _, value := range inputCol {
		av.Avg += float64(value)
		av.Count++
	}
	return nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (m Avg) New() (out uda.AggInterface, am *functions.ArgumentMap) {
	av := NewCount(requiredColumns, optionalColumns)
	return av, av.ArgMap
}

/*
CONCRETE - these may be suitable methods for general usage
*/
func NewCount(inputColumns, optionalInputColumns []io.DataShape) (av *Avg) {
	av = new(Avg)
	av.ArgMap = functions.NewArgumentMap(inputColumns, optionalInputColumns...)
	return av
}
func (av *Avg) Init(itf ...interface{}) error {
	if unmapped := av.ArgMap.Validate(); unmapped != nil {
		return fmt.Errorf("Unmapped columns: %s", unmapped)
	}
	av.Avg = 0
	av.Count = 0
	return nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (av *Avg) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().UTC().Unix()})
	cs.AddColumn("Avg", []float64{av.Avg / float64(av.Count)})
	return cs
}
