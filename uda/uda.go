package uda

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// ColumnToFloat32 converts the specified column to a slice of float32.
// nolint: dupl // generics can make the code slower
func ColumnToFloat32(cols io.ColumnInterface, name string) (outCol []float32, err error) {
	// nolint:ifshort // false positive
	ccol := cols.GetColumn(name)
	if ccol == nil {
		return nil, fmt.Errorf("unable to retrieve column named %s", name)
	}
	switch cc := ccol.(type) {
	case []float32:
		outCol = cc
	case []float64:
		outCol = make([]float32, len(cc))
		for i := range cc {
			outCol[i] = float32(cc[i])
		}
	case []int:
		outCol = make([]float32, len(cc))
		for i := range cc {
			outCol[i] = float32(cc[i])
		}
	case []int64:
		outCol = make([]float32, len(cc))
		for i := range cc {
			outCol[i] = float32(cc[i])
		}
	case []int32:
		outCol = make([]float32, len(cc))
		for i := range cc {
			outCol[i] = float32(cc[i])
		}
	}
	return outCol, nil
}

// ColumnToFloat64 converts the specified column to a slice of float64.
// nolint: dupl // generics can make the code slower
func ColumnToFloat64(cols io.ColumnInterface, name string) (outCol []float64, err error) {
	// nolint:ifshort // false positive
	ccol := cols.GetColumn(name)
	if ccol == nil {
		return nil, fmt.Errorf("unable to retrieve column named %s", name)
	}
	switch cc := ccol.(type) {
	case []float64:
		outCol = cc
	case []float32:
		outCol = make([]float64, len(cc))
		for i := range cc {
			outCol[i] = float64(cc[i])
		}
	case []int:
		outCol = make([]float64, len(cc))
		for i := range cc {
			outCol[i] = float64(cc[i])
		}
	case []int64:
		outCol = make([]float64, len(cc))
		for i := range cc {
			outCol[i] = float64(cc[i])
		}
	case []int32:
		outCol = make([]float64, len(cc))
		for i := range cc {
			outCol[i] = float64(cc[i])
		}
	}
	return outCol, nil
}
