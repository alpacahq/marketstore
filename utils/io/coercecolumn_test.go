package io

import (
	"testing"
)

const columnName = "columnName"

func TestColumnSeries_CoerceColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addColumnFunc func(cs *ColumnSeries)
		newColumnType EnumElementType
		wantErr       bool
	}{
		{
			name:          "int to int",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: INT64,
			wantErr:       false,
		},
		{
			name:          "int to byte",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: BYTE,
			wantErr:       false,
		},
		{
			name:          "int to float",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int32{1, 2, 3}) },
			newColumnType: FLOAT32,
			wantErr:       false,
		},
		{
			name:          "int to uint",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int32{1, 2, 3}) },
			newColumnType: UINT64,
			wantErr:       false,
		},
		{
			name:          "float to int",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []float64{1, 2, 3}) },
			newColumnType: INT32,
			wantErr:       false,
		},
		{
			name:          "float to float",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []float32{1, 2, 3}) },
			newColumnType: FLOAT64,
			wantErr:       false,
		},
		{
			name:          "float to uint",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []float64{1, 2, 3}) },
			newColumnType: UINT32,
			wantErr:       false,
		},
		{
			name:          "uint to int",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: INT16,
			wantErr:       false,
		},
		{
			name:          "uint to float",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: FLOAT64,
			wantErr:       false,
		},
		{
			name:          "uint to uint16",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: UINT16,
			wantErr:       false,
		},
		{
			name:          "uint to uint8",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []uint{1, 2, 3}) },
			newColumnType: UINT8,
			wantErr:       false,
		},
		{
			name:          "error/STRING is not supported",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: STRING,
			wantErr:       true,
		},
		{
			name:          "error/BOOL is not supported",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, []int{1, 2, 3}) },
			newColumnType: BOOL,
			wantErr:       true,
		},
		{
			name:          "error/corrupted(not iterable) column is specified",
			addColumnFunc: func(cs *ColumnSeries) { cs.AddColumn(columnName, "foobar") },
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// prepare columnSeries
			cs := NewColumnSeries()
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

// assertType fails if the column values can't be asserted as a specified ElementType
func assertType(t *testing.T, cs *ColumnSeries, typ EnumElementType) {
	t.Helper()

	var ok bool
	switch typ {
	case BYTE:
		_, ok = cs.GetByName(columnName).([]byte)
	case INT16:
		_, ok = cs.GetByName(columnName).([]int16)
	case INT32:
		_, ok = cs.GetByName(columnName).([]int32)
	case INT64:
		_, ok = cs.GetByName(columnName).([]int64)
	case FLOAT32:
		_, ok = cs.GetByName(columnName).([]float32)
	case FLOAT64:
		_, ok = cs.GetByName(columnName).([]float64)
	case UINT8:
		_, ok = cs.GetByName(columnName).([]uint8)
	case UINT16:
		_, ok = cs.GetByName(columnName).([]uint16)
	case UINT32:
		_, ok = cs.GetByName(columnName).([]uint32)
	case UINT64:
		_, ok = cs.GetByName(columnName).([]uint64)
	}
	if !ok {
		t.Fatal("column type not coerced expectedly")
	}
}
