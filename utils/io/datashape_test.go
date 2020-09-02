package io_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestDSVSerialize(t *testing.T) {
	tests := []struct {
		name        string
		columnNames []string
		elemTypes   []io.EnumElementType
	}{
		{
			name:        "success",
			columnNames: []string{"column1", "column2"},
			elemTypes:   []io.EnumElementType{io.FLOAT32, io.UINT64},
		},
		{
			name:        "success/empty DataShape",
			columnNames: []string{},
			elemTypes:   []io.EnumElementType{},
		},
		{
			name:        "success/single column",
			columnNames: []string{"column"},
			elemTypes:   []io.EnumElementType{io.EPOCH},
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			dsv := io.NewDataShapeVector(tt.columnNames, tt.elemTypes)

			serialized, err := io.DSVToBytes(dsv)
			if err != nil {
				t.Fatalf("failed to serialize DSV: " + err.Error())
			}

			deserialized, _ := io.DSVFromBytes(serialized)
			if diff := cmp.Diff(dsv, deserialized); diff != "" {
				t.Errorf("Original DSV/Serialized->Deserialized DSV mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
