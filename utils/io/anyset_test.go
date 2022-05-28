package io

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnySet(t *testing.T) {
	A := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	B := []string{"4", "5", "6", "22"}
	D := []string{"4", "5", "6", "11", "20"}
	AintB := []string{"4", "5", "6"}
	result1 := []string{"1", "a", "3", "7", "8", "9", "10"}
	/*
		Strings
	*/
	sgA, err := NewAnySet(A)
	assert.Nil(t, err)
	assert.Equal(t, AintB, sgA.Intersect(B))
	assert.Equal(t, result1, sgA.Subtract(B))
	assert.Equal(t, result1, sgA.Subtract(D))
	assert.False(t, sgA.Contains(B))
	assert.True(t, sgA.Contains(AintB))
	Anoa := []string{"1", "3", "4", "5", "6", "7", "8", "9", "10"}
	var Empty []string
	sgA.Del("a")
	assert.Equal(t, sgA.Subtract(Anoa), Empty)
	sgA.Add("2020202020")
	twenty := []string{"2020202020"}
	assert.Equal(t, sgA.Subtract(Anoa), twenty)

	/*
		Integers
	*/
	AA := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	BB := []int{4, 5, 6, 22}
	DD := []int{4, 5, 6, 11, 20}
	AAintBB := []int{4, 5, 6}
	result11 := []int{1, 2, 3, 7, 8, 9, 10}
	sgAA, err := NewAnySet(AA)
	assert.Nil(t, err)
	assert.Equal(t, AAintBB, sgAA.Intersect(BB))
	assert.Equal(t, result11, sgAA.Subtract(BB))
	assert.Equal(t, result11, sgAA.Subtract(DD))
	assert.False(t, sgAA.Contains(BB))
	assert.True(t, sgAA.Contains(AAintBB))
	AAnoa := []int{1, 3, 4, 5, 6, 7, 8, 9, 10}
	var IEmpty []int
	Itwenty := []int{2020202020}
	sgAA.Del(2)
	assert.Equal(t, sgAA.Subtract(AAnoa), IEmpty)
	sgAA.Add(2020202020)
	assert.Equal(t, sgAA.Subtract(AAnoa), Itwenty)

	/*
		Datashapes
	*/
	AAA := []DataShape{
		{Name: "1", Type: FLOAT32},
		{Name: "2", Type: FLOAT32},
		{Name: "3", Type: FLOAT32},
		{Name: "4", Type: FLOAT32},
		{Name: "5", Type: FLOAT32},
		{Name: "6", Type: FLOAT32},
		{Name: "7", Type: FLOAT32},
		{Name: "8", Type: FLOAT32},
		{Name: "9", Type: FLOAT32},
		{Name: "10", Type: FLOAT32},
	}
	BBB := []DataShape{
		{Name: "4", Type: FLOAT32},
		{Name: "5", Type: FLOAT32},
		{Name: "6", Type: FLOAT32},
		{Name: "22", Type: FLOAT32},
	}
	DDD := []DataShape{
		{Name: "4", Type: FLOAT32},
		{Name: "5", Type: FLOAT32},
		{Name: "6", Type: FLOAT32},
		{Name: "11", Type: FLOAT32},
		{Name: "20", Type: FLOAT32},
	}
	AAAintBBB := []DataShape{
		{Name: "4", Type: FLOAT32},
		{Name: "5", Type: FLOAT32},
		{Name: "6", Type: FLOAT32},
	}
	result2 := []DataShape{
		{Name: "1", Type: FLOAT32},
		{Name: "2", Type: FLOAT32},
		{Name: "3", Type: FLOAT32},
		{Name: "7", Type: FLOAT32},
		{Name: "8", Type: FLOAT32},
		{Name: "9", Type: FLOAT32},
		{Name: "10", Type: FLOAT32},
	}
	sgAAA, err := NewAnySet(AAA)
	assert.Nil(t, err)
	assert.Equal(t, AAAintBBB, sgAAA.Intersect(BBB))
	assert.Equal(t, result2, sgAAA.Subtract(BBB))
	assert.Equal(t, result2, sgAAA.Subtract(DDD))
	assert.False(t, sgAAA.Contains(BBB))
	assert.True(t, sgAAA.Contains(AAAintBBB))
	AAAnoa := []DataShape{
		{Name: "1", Type: FLOAT32},
		{Name: "2", Type: FLOAT32},
		{Name: "3", Type: FLOAT32},
		{Name: "4", Type: FLOAT32},
		{Name: "5", Type: FLOAT32},
		{Name: "6", Type: FLOAT32},
		{Name: "7", Type: FLOAT32},
		{Name: "8", Type: FLOAT32},
		{Name: "9", Type: FLOAT32},
		{Name: "10", Type: FLOAT32},
	}
	var I2Empty []DataShape
	I2twenty := []DataShape{
		{Name: "2020202020", Type: FLOAT64},
	}
	I2twentyFloat32 := []DataShape{
		{Name: "2020202020", Type: FLOAT32},
	}
	sgAAA.Del(DataShape{Name: "2", Type: FLOAT32})
	assert.Equal(t, sgAAA.Subtract(AAAnoa), I2Empty)
	sgAAA.Add(DataShape{Name: "2020202020", Type: FLOAT64})
	assert.Equal(t, sgAAA.Subtract(AAAnoa), I2twenty)
	sgAAA.Del(DataShape{Name: "2020202020", Type: FLOAT64})
	assert.Equal(t, sgAAA.Subtract(AAAnoa), I2Empty)
	sgAAA.Add(DataShape{Name: "2020202020", Type: FLOAT32})
	assert.Equal(t, sgAAA.Subtract(AAAnoa), I2twentyFloat32)
}

func TestGetMissingColumns(t *testing.T) {
	col1 := []float32{1, 2, 3}
	col2 := []float64{1, 2, 3}
	col3 := []int32{1, 2, 3}
	col4 := []int64{1, 2, 3}
	col5 := []int64{1, 2, 3}
	csA := NewColumnSeries()
	csA.AddColumn("One", col1)
	csA.AddColumn("Two", col2)
	csA.AddColumn("Three", col3)
	csA.AddColumn("Four", col4)
	csA.AddColumn("Five", col5)
	assert.Equal(t, csA.Len(), 3)
	names := []string{"One", "Two", "Three", "Four", "Five"}
	types := []EnumElementType{FLOAT32, FLOAT64, INT32, INT64, INT64}
	requiredDSV := NewDataShapeVector(names, types)
	columnDSVSet, err := NewAnySet(csA.GetDataShapes())
	assert.Nil(t, err)
	assert.True(t, columnDSVSet.Contains(requiredDSV))
	/*
		All columns are present
	*/
	missing, coercion, err := GetMissingAndTypeCoercionColumns(
		requiredDSV,
		csA.GetDataShapes(),
	)
	assert.Nil(t, err)
	assert.Len(t, missing, 0)
	assert.Len(t, coercion, 0)

	assert.Nil(t, csA.Remove("Three"))
	/*
		We have a missing column
	*/
	missing, coercion, err = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	assert.Nil(t, err)
	assert.Len(t, missing, 1)
	assert.Len(t, coercion, 0)

	csA.AddColumn("Three", col2)
	/*
		Now we have a mismatch of a column's type with the same name
	*/
	missing, coercion, err = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	assert.Nil(t, err)
	assert.Len(t, missing, 0)
	assert.Len(t, coercion, 1)

	ds6 := DataShape{Name: "Six", Type: FLOAT32}
	requiredDSV = append(requiredDSV, ds6)
	/*
		We added an extra column to the required, so we should report a new
		missing
	*/
	missing, coercion, err = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	assert.Nil(t, err)
	assert.Len(t, missing, 1)
	assert.Len(t, coercion, 1)
	assert.Equal(t, missing[0], ds6)
	assert.Equal(t, coercion[0], DataShape{Name: "Three", Type: INT32})
}
