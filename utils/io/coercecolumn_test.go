package io_test

import (
	"github.com/alpacahq/marketstore/v4/utils/io"
	"testing"
)

const columnName = "columnName"

func TestColumnSeries_CoerceColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addColumnFunc func(cs *io.ColumnSeries)
		newColumnType io.EnumElementType
		wantErr       bool
	}{
		{
			name:          "int to int",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: io.INT64,
			wantErr:       false,
		},
		{
			name:          "int to byte",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: io.BYTE,
			wantErr:       false,
		},
		{
			name:          "int to float",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int32{1, 2, 3}) },
			newColumnType: io.FLOAT32,
			wantErr:       false,
		},
		{
			name:          "int to uint",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int32{1, 2, 3}) },
			newColumnType: io.UINT64,
			wantErr:       false,
		},
		{
			name:          "float to int",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []float64{1, 2, 3}) },
			newColumnType: io.INT32,
			wantErr:       false,
		},
		{
			name:          "float to float",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []float32{1, 2, 3}) },
			newColumnType: io.FLOAT64,
			wantErr:       false,
		},
		{
			name:          "float to uint",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []float64{1, 2, 3}) },
			newColumnType: io.UINT32,
			wantErr:       false,
		},
		{
			name:          "uint to int",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: io.INT16,
			wantErr:       false,
		},
		{
			name:          "uint to float",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: io.FLOAT64,
			wantErr:       false,
		},
		{
			name:          "uint to uint16",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: io.UINT16,
			wantErr:       false,
		},
		{
			name:          "uint to uint8",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: io.UINT8,
			wantErr:       false,
		},
		{
			name:          "error/STRING is not supported",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: io.STRING,
			wantErr:       true,
		},
		{
			name:          "error/STRING16 is not supported",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: io.STRING16,
			wantErr:       true,
		},
		{
			name:          "error/BOOL is not supported",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: io.BOOL,
			wantErr:       true,
		},
		{
			name:          "error/corrupted(not iterable) column is specified",
			addColumnFunc: func(cs *io.ColumnSeries) { cs.AddColumn(columnName, "foobar") },
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// prepare columnSeries
			cs := io.NewColumnSeries()
			tt.addColumnFunc(cs)

			if err := cs.CoerceColumnType(columnName, tt.newColumnType); (err != nil) != tt.wantErr {
				t.Errorf("CoerceColumnType() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				assertType(t, cs, tt.newColumnType)
			}
		})
	}
}

// assertType fails if the column values can't be asserted as a specified ElementType.
func assertType(t *testing.T, cs *io.ColumnSeries, typ io.EnumElementType) {
	t.Helper()

	var ok bool
	switch typ {
	case io.BYTE:
		_, ok = cs.GetByName(columnName).([]byte)
	case io.INT16:
		_, ok = cs.GetByName(columnName).([]int16)
	case io.INT32:
		_, ok = cs.GetByName(columnName).([]int32)
	case io.INT64:
		_, ok = cs.GetByName(columnName).([]int64)
	case io.FLOAT32:
		_, ok = cs.GetByName(columnName).([]float32)
	case io.FLOAT64:
		_, ok = cs.GetByName(columnName).([]float64)
	case io.UINT8:
		_, ok = cs.GetByName(columnName).([]uint8)
	case io.UINT16:
		_, ok = cs.GetByName(columnName).([]uint16)
	case io.UINT32:
		_, ok = cs.GetByName(columnName).([]uint32)
	case io.UINT64:
		_, ok = cs.GetByName(columnName).([]uint64)
	}
	if !ok {
		t.Fatal("column type not coerced expectedly")
	}
}
