package io

import (
	"math"
	"reflect"
	"testing"
	"time"
	"unsafe"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

type ohlc struct {
	index      int64
	o, h, l, c float32
}

func (s *TestSuite) TestConvertByteSlice(c *C) {
	s1 := ohlc{100000000, 200, 300, 400, 500}
	s2 := ohlc{10, 20, 30, 40, 50}
	baseArray := [2]ohlc{s1, s2}
	buffer := make([]byte, 48)
	copy(buffer, (*(*[24]byte)(unsafe.Pointer(&s1)))[:])
	copy(buffer[24:], (*(*[24]byte)(unsafe.Pointer(&s2)))[:])

	i_myarray := CopySliceByte(buffer, ohlc{})
	myarray := i_myarray.([]ohlc)
	for i, ohlc := range myarray {
		c.Assert(ohlc, Equals, baseArray[i])
	}

	i_myarray = SwapSliceByte(buffer, ohlc{})
	myarray = i_myarray.([]ohlc)
	for i, ohlc := range myarray {
		c.Assert(ohlc, Equals, baseArray[i])
	}

	i_mybyte := SwapSliceData(myarray, byte(0))
	bytearray := i_mybyte.([]byte)
	c.Assert(len(bytearray), Equals, 48)
	for i, val := range bytearray {
		c.Assert(val, Equals, buffer[i])
	}

	bytearray = CastToByteSlice(myarray)
	c.Assert(len(bytearray), Equals, 48)
	for i, val := range bytearray {
		c.Assert(val, Equals, buffer[i])
	}

	i_myarray = SwapSliceData(buffer, ohlc{})
	myarray = i_myarray.([]ohlc)
	c.Assert(len(myarray), Equals, 2)
	for i, val := range myarray {
		c.Assert(val, Equals, baseArray[i])
	}

	myInt64 := int64(65793)
	bs := DataToByteSlice(myInt64)
	c.Assert(len(bs), Equals, 8)
	for i, val := range []byte{1, 1, 1, 0, 0, 0, 0, 0} {
		c.Assert(bs[i] == val, Equals, true)
	}
}

func (s *TestSuite) TestVariableBoundaryCases(c *C) {
	/*
		Reported problematic times:
			Input:
			  20080103 16:24:03 255970,2495.0,2495.0
			  20080103 16:24:59 839106,2495.0,2495.0
			Output from query at 1Min TF:
			  2008-01-03 16:24:04 +0000 UTC	2495	2495	255969995
			  2008-01-03 16:24:00 +0000 UTC	2495	2495	839105998

	*/
	t1 := time.Date(2008, time.January, 3, 16, 24, 3, 1000*255970, time.UTC)
	//fmt.Println("Test Time:  ", t1, " Minutes: ", t1.Minute(), " Seconds: ", t1.Second())

	// Check the 1Min interval
	index := TimeToIndex(t1, 1440)
	o_t1 := IndexToTime(index, 1440, 2008)
	//fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	c.Assert(o_t1.Minute(), Equals, 24)
	c.Assert(o_t1.Second(), Equals, 0)
	ticks := GetIntervalTicks32Bit(t1, index, 1440)
	seconds := t1.Second()
	nanos := t1.Nanosecond()
	fractionalSeconds := float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval := fractionalSeconds / 60.
	intervalTicks := uint32(fractionalInterval * math.MaxUint32)
	//fmt.Println("Interval Ticks: ", intervalTicks)
	c.Assert(intervalTicks, Equals, ticks)

	t1 = time.Date(2008, time.January, 3, 16, 24, 59, 1000*839106, time.UTC)

	index = TimeToIndex(t1, 1440)
	o_t1 = IndexToTime(index, 1440, 2008)
	//fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	c.Assert(o_t1.Minute(), Equals, 24)
	c.Assert(o_t1.Second(), Equals, 0)
	ticks = GetIntervalTicks32Bit(t1, index, 1440)
	seconds = t1.Second()
	nanos = t1.Nanosecond()
	fractionalSeconds = float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval = fractionalSeconds / 60.
	intervalTicks = uint32(fractionalInterval * math.MaxUint32)
	//fmt.Println("Interval Ticks: ", intervalTicks, " Ticks: ", ticks)
	diff := int64(intervalTicks) - int64(ticks)
	if diff < 0 {
		diff = -diff
	}
	c.Assert(diff < 2, Equals, true)
}

func (s *TestSuite) TestQuorumValue(c *C) {
	qv := NewQuorumValue()
	A := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AAA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AAAA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AAAAA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AAAAAA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	AAAAAAA := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	B := []string{"4", "5", "6", "22"}
	CC := []string{"1", "a", "3", "4", "5", "6", "7", "8", "9", "10"}
	D := []string{"4", "5", "6", "11", "20"}
	DD := []string{"4", "5", "6", "11", "20"}
	DDD := []string{"4", "5", "6", "11", "20"}
	DDDD := []string{"4", "5", "6", "11", "20"}
	DDDDD := []string{"4", "5", "6", "11", "20"}

	qv.AddValue(A)
	qv.AddValue(AA)
	qv.AddValue(B)
	qv.AddValue(B)
	qv.AddValue(B)
	qv.AddValue(CC)
	qv.AddValue(D)
	qv.AddValue(DD)
	qv.AddValue(DDD)
	qv.AddValue(DDDD)
	qv.AddValue(DDDDD)
	val, conf := qv.GetTopValue()
	values := val.([]string)
	c.Assert(reflect.DeepEqual(values, D), Equals, true)
	c.Assert(conf, Equals, 5)

	qv.AddValue(AAA)
	qv.AddValue(AAAA)
	qv.AddValue(AAAAA)
	qv.AddValue(AAAAAA)
	qv.AddValue(AAAAAAA)

	val, conf = qv.GetTopValue()
	values = val.([]string)
	c.Assert(reflect.DeepEqual(values, A), Equals, true)
	c.Assert(conf, Equals, 8)
}

func (s *TestSuite) TestGenerics(c *C) {
	// DownSizeSlice
	input := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	output, err := DownSizeSlice(input, 6, LAST)
	expected := []float64{5, 6, 7, 8, 9, 10}
	c.Assert(err, Equals, nil)
	c.Assert(reflect.DeepEqual(output, expected), Equals, true)

	output, err = DownSizeSlice(input, 6, FIRST)
	expected = []float64{1, 2, 3, 4, 5, 6}
	c.Assert(err, Equals, nil)
	c.Assert(reflect.DeepEqual(output, expected), Equals, true)

	input2 := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	output2, err := DownSizeSlice(input2, 6, FIRST)
	expected2 := []uint32{1, 2, 3, 4, 5, 6}
	c.Assert(err, Equals, nil)
	c.Assert(reflect.DeepEqual(output2, expected2), Equals, true)

	input3 := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	output3, err := DownSizeSlice(input3, 6, FIRST)
	expected3 := []string{"1", "2", "3", "4", "5", "6"}
	c.Assert(err, Equals, nil)
	c.Assert(reflect.DeepEqual(output3, expected3), Equals, true)

	/*
		Test the error case
	*/
	_, err = DownSizeSlice("Should not work", 100, FIRST)
	c.Assert(err != nil, Equals, true)
}

func (s *TestSuite) TestColumnCoercion(c *C) {
	col1 := []float32{1, 2, 3}
	col2 := []float64{1, 2, 3}
	col3 := []int32{1, 2, 3}
	col4 := []int64{1, 2, 3}
	col5 := []int8{1, 2, 3}
	csA := NewColumnSeries()
	csA.AddColumn("One", col1)
	csA.AddColumn("Two", col2)
	csA.AddColumn("Three", col3)
	csA.AddColumn("Four", col4)
	csA.AddColumn("Five", col5)
	c.Assert(csA.Len(), Equals, 3)

	dsNew := DataShape{Name: "Three", Type: FLOAT32}
	csA.CoerceColumnType(dsNew)
	_, ok := csA.GetByName("Three").([]float32)
	c.Assert(ok, Equals, true)

	dsNew = DataShape{Name: "Three", Type: FLOAT64}
	csA.CoerceColumnType(dsNew)
	_, ok = csA.GetByName("Three").([]float64)
	c.Assert(ok, Equals, true)

	dsNew = DataShape{Name: "Three", Type: INT32}
	csA.CoerceColumnType(dsNew)
	_, ok = csA.GetByName("Three").([]int32)
	c.Assert(ok, Equals, true)

	dsNew = DataShape{Name: "Three", Type: INT64}
	csA.CoerceColumnType(dsNew)
	_, ok = csA.GetByName("Three").([]int64)
	c.Assert(ok, Equals, true)

	dsNew = DataShape{Name: "Three", Type: BYTE}
	csA.CoerceColumnType(dsNew)
	_, ok = csA.GetByName("Three").([]int8)
	c.Assert(ok, Equals, true)
}

func makeTestCS() *ColumnSeries {
	col1 := []float32{1, 2, 3}
	col2 := []float64{1, 2, 3}
	col3 := []int32{1, 2, 3}
	col4 := []int64{1, 2, 3}
	col5 := []byte{1, 2, 3}
	csA := NewColumnSeries()
	csA.AddColumn("Epoch", col4)
	csA.AddColumn("One", col1)
	csA.AddColumn("Two", col2)
	csA.AddColumn("Three", col3)
	csA.AddColumn("Four", col4)
	csA.AddColumn("Five", col5)
	return csA
}

func (s *TestSuite) TestSerializeColumnsToRows(c *C) {
	csA := makeTestCS()
	c.Assert(csA.Len(), Equals, 3)

	dsv := csA.GetDataShapes()

	/*
		Unaligned case
	*/
	UnalignedBytesPerRow := 8 + 4 + 8 + 4 + 8 + 1
	data, reclen := SerializeColumnsToRows(csA, dsv, false)
	c.Assert(reclen == UnalignedBytesPerRow, Equals, true)
	c.Assert(reclen*3 == len(data), Equals, true)

	/*
		Aligned case
	*/
	AlignedBytesPerRow := AlignedSize(UnalignedBytesPerRow)
	data, reclen = SerializeColumnsToRows(csA, dsv, true)
	c.Assert(reclen == AlignedBytesPerRow, Equals, true)
	c.Assert(reclen*3 == len(data), Equals, true)

	/*
		Projection case
	*/
	csB := makeTestCS()
	csB.Remove("Three")
	dsvProjected := csB.GetDataShapes()

	// Expected record length
	var expectedLen int
	for _, shape := range dsvProjected {
		expectedLen += shape.Len()
	}
	expectedLen = AlignedSize(expectedLen)

	data, reclen = SerializeColumnsToRows(csA, dsvProjected, true)
	c.Assert(reclen == expectedLen, Equals, true)
	c.Assert(reclen*3 == len(data), Equals, true)
	/*
		Type Coercion case
	*/
	newDS := DataShape{Name: "Two", Type: BYTE} // Is currently FLOAT64
	csB.CoerceColumnType(newDS)
	dsvProjected = csB.GetDataShapes()

	// Expected record length
	expectedLen = 0
	for _, shape := range dsvProjected {
		expectedLen += shape.Len()
	}
	expectedLen = AlignedSize(expectedLen)

	data, reclen = SerializeColumnsToRows(csA, dsvProjected, true)
	c.Assert(reclen == expectedLen, Equals, true)
	c.Assert(reclen*3 == len(data), Equals, true)
}
