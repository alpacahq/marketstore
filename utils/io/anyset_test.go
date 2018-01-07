package io

import (
	"reflect"

	. "gopkg.in/check.v1"
)

type TestSuite2 struct{}

var _ = Suite(&TestSuite2{})

func (s *TestSuite2) TestAnySet(c *C) {
	A := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	B := []string{"4", "5", "6", "22"}
	D := []string{"4", "5", "6", "11", "20"}
	AintB := []string{"4", "5", "6"}
	result1 := []string{"1", "a", "3", "7", "8", "9", "10"}
	/*
		Strings
	*/
	sgA, err := NewAnySet(A)
	c.Assert(err == nil, Equals, true)
	c.Assert(reflect.DeepEqual(AintB, sgA.Intersect(B)), Equals, true)
	c.Assert(reflect.DeepEqual(result1, sgA.Subtract(B)), Equals, true)
	c.Assert(reflect.DeepEqual(result1, sgA.Subtract(D)), Equals, true)
	c.Assert(sgA.Contains(B), Equals, false)
	c.Assert(sgA.Contains(AintB), Equals, true)
	Anoa := []string{"1", "3", "4", "5", "6", "7", "8", "9", "10"}
	Empty := []string{}
	sgA.Del("a")
	c.Assert(reflect.DeepEqual(sgA.Subtract(Anoa), Empty), Equals, true)
	sgA.Add("2020202020")
	twenty := []string{"2020202020"}
	c.Assert(reflect.DeepEqual(sgA.Subtract(Anoa), twenty), Equals, true)

	/*
		Integers
	*/
	AA := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	BB := []int{4, 5, 6, 22}
	DD := []int{4, 5, 6, 11, 20}
	AAintBB := []int{4, 5, 6}
	result11 := []int{1, 2, 3, 7, 8, 9, 10}
	sgAA, err := NewAnySet(AA)
	c.Assert(err == nil, Equals, true)
	c.Assert(reflect.DeepEqual(AAintBB, sgAA.Intersect(BB)), Equals, true)
	c.Assert(reflect.DeepEqual(result11, sgAA.Subtract(BB)), Equals, true)
	c.Assert(reflect.DeepEqual(result11, sgAA.Subtract(DD)), Equals, true)
	c.Assert(sgAA.Contains(BB), Equals, false)
	c.Assert(sgAA.Contains(AAintBB), Equals, true)
	AAnoa := []int{1, 3, 4, 5, 6, 7, 8, 9, 10}
	IEmpty := []int{}
	Itwenty := []int{2020202020}
	sgAA.Del(2)
	c.Assert(reflect.DeepEqual(sgAA.Subtract(AAnoa), IEmpty), Equals, true)
	sgAA.Add(2020202020)
	c.Assert(reflect.DeepEqual(sgAA.Subtract(AAnoa), Itwenty), Equals, true)

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
	c.Assert(err == nil, Equals, true)
	c.Assert(reflect.DeepEqual(AAAintBBB, sgAAA.Intersect(BBB)), Equals, true)
	c.Assert(reflect.DeepEqual(result2, sgAAA.Subtract(BBB)), Equals, true)
	c.Assert(reflect.DeepEqual(result2, sgAAA.Subtract(DDD)), Equals, true)
	c.Assert(sgAAA.Contains(BBB), Equals, false)
	c.Assert(sgAAA.Contains(AAAintBBB), Equals, true)
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
	I2Empty := []DataShape{}
	I2twenty := []DataShape{
		{Name: "2020202020", Type: FLOAT64},
	}
	I2twentyFloat32 := []DataShape{
		{Name: "2020202020", Type: FLOAT32},
	}
	sgAAA.Del(DataShape{Name: "2", Type: FLOAT32})
	c.Assert(reflect.DeepEqual(sgAAA.Subtract(AAAnoa), I2Empty), Equals, true)
	sgAAA.Add(DataShape{Name: "2020202020", Type: FLOAT64})
	c.Assert(reflect.DeepEqual(sgAAA.Subtract(AAAnoa), I2twenty), Equals, true)
	sgAAA.Del(DataShape{Name: "2020202020", Type: FLOAT64})
	c.Assert(reflect.DeepEqual(sgAAA.Subtract(AAAnoa), I2Empty), Equals, true)
	sgAAA.Add(DataShape{Name: "2020202020", Type: FLOAT32})
	c.Assert(reflect.DeepEqual(sgAAA.Subtract(AAAnoa), I2twentyFloat32), Equals, true)

}

func (s *TestSuite2) TestGetMissingColumns(c *C) {
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
	c.Assert(csA.Len(), Equals, 3)
	names := []string{"One", "Two", "Three", "Four", "Five"}
	types := []EnumElementType{FLOAT32, FLOAT64, INT32, INT64, INT64}
	requiredDSV := NewDataShapeVector(names, types)
	columnDSVSet, err := NewAnySet(csA.GetDataShapes())
	c.Assert(err == nil, Equals, true)
	c.Assert(columnDSVSet.Contains(requiredDSV), Equals, true)
	/*
		All columns are present
	*/
	missing, coercion := GetMissingAndTypeCoercionColumns(
		requiredDSV,
		csA.GetDataShapes(),
	)
	c.Assert(len(missing) == 0, Equals, true)
	c.Assert(len(coercion) == 0, Equals, true)

	csA.Remove("Three")
	/*
		We have a missing column
	*/
	missing, coercion = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	c.Assert(len(missing) == 1, Equals, true)
	c.Assert(len(coercion) == 0, Equals, true)

	csA.AddColumn("Three", col2)
	/*
		Now we have a mismatch of a column's type with the same name
	*/
	missing, coercion = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	c.Assert(len(missing) == 0, Equals, true)
	c.Assert(len(coercion) == 1, Equals, true)

	ds6 := DataShape{Name: "Six", Type: FLOAT32}
	requiredDSV = append(requiredDSV, ds6)
	/*
		We added an extra column to the required, so we should report a new
		missing
	*/
	missing, coercion = GetMissingAndTypeCoercionColumns(requiredDSV,
		csA.GetDataShapes())
	c.Assert(len(missing) == 1, Equals, true)
	c.Assert(len(coercion) == 1, Equals, true)
	c.Assert(missing[0], Equals, ds6)
	c.Assert(coercion[0], Equals, DataShape{Name: "Three", Type: INT32})
}
