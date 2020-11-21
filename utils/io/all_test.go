package io

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/alpacahq/marketstore/v4/utils"
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
	index := TimeToIndex(t1, time.Minute)
	o_t1 := IndexToTime(index, time.Minute, 2008)
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

	index = TimeToIndex(t1, time.Minute)
	o_t1 = IndexToTime(index, time.Minute, 2008)
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
	csB.CoerceColumnType("Two", BYTE) // Is currently FLOAT64
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

func (s *TestSuite) TestTimeBucketInfo(c *C) {
	tempDir := c.MkDir()
	timeframe := utils.NewTimeframe("1Min")
	filePath := filepath.Join(tempDir, "2018.bin")
	description := "testing"
	year := int16(2018)
	dsv := NewDataShapeVector([]string{
		"Open", "Close",
	}, []EnumElementType{
		FLOAT32, FLOAT32,
	})
	recType := FIXED
	tbi := NewTimeBucketInfo(*timeframe, filePath, description, year, dsv, recType)

	testFilePath, err := os.Create(filePath)
	c.Check(err, IsNil)
	WriteHeader(testFilePath, tbi)

	tbi2 := TimeBucketInfo{
		Year: year,
		Path: filePath,
	}

	nElements := tbi2.GetNelements()
	c.Check(nElements, Equals, int32(2))

	c.Check(tbi2.GetVariableRecordLength(), Equals, int32(0))

	fcopy := tbi2.GetDeepCopy()
	c.Check(fcopy.timeframe.Nanoseconds(), Equals, tbi.timeframe.Nanoseconds())

	dsv2 := tbi2.GetDataShapes()
	c.Check(dsv2[0].String(), Equals, dsv[0].String())
	c.Check(dsv2[0].Equal(dsv[0]), Equals, true)
}

func (s *TestSuite) TestIndexAndOffset(c *C) {
	recSize := int32(28)
	loc, _ := time.LoadLocation("America/New_York")
	utils.InstanceConfig.Timezone = loc

	// check the 1Min interval
	t0 := time.Date(2018, time.January, 1, 0, 0, 0, 0, loc)
	index := TimeToIndex(t0, time.Minute)
	c.Assert(index, Equals, int64(1))
	o_t0 := IndexToTime(index, time.Minute, 2018)
	c.Assert(o_t0, Equals, t0)

	offset := TimeToOffset(t0, time.Minute, recSize)
	o_o0 := IndexToOffset(index, recSize)
	c.Assert(o_o0, Equals, offset)

	epoch := t0.Unix()
	c.Assert(EpochToOffset(epoch, time.Minute, recSize), Equals, offset)

	// Check the 5Min interval
	t1 := time.Date(2018, time.January, 1, 0, 5, 0, 0, loc)
	index = TimeToIndex(t1, 5*time.Minute)
	c.Assert(index, Equals, int64(2))
	o_t1 := IndexToTime(index, 5*time.Minute, 2018)
	c.Assert(o_t1, Equals, t1)

	offset = TimeToOffset(t1, 5*time.Minute, recSize)
	o_o1 := IndexToOffset(index, recSize)
	c.Assert(o_o1, Equals, offset)

	epoch = t1.Unix()
	c.Assert(EpochToOffset(epoch, 5*time.Minute, recSize), Equals, offset)

	// Check the 1D interval
	t2 := time.Date(2018, time.February, 5, 0, 0, 0, 0, loc)
	index = TimeToIndex(t2, utils.Day)
	c.Assert(index, Equals, int64(35))
	o_t2 := IndexToTime(index, utils.Day, 2018)
	c.Assert(o_t2, Equals, t2)

	offset = TimeToOffset(t2, utils.Day, recSize)
	o_o2 := IndexToOffset(index, recSize)
	c.Assert(o_o2, Equals, offset)

	epoch = t2.Unix()
	c.Assert(EpochToOffset(epoch, utils.Day, recSize), Equals, offset)

	// Check 1D at end of year
	t3 := time.Date(2018, time.December, 31, 0, 0, 0, 0, loc)
	index = TimeToIndex(t3, utils.Day)
	c.Assert(index, Equals, int64(364))
	o_t3 := IndexToTime(index, utils.Day, 2018)
	c.Assert(o_t3, Equals, t3)

	offset = TimeToOffset(t3, utils.Day, recSize)
	o_o3 := IndexToOffset(index, recSize)
	c.Assert(o_o3, Equals, offset)

	epoch = t3.Unix()
	c.Assert(EpochToOffset(epoch, utils.Day, recSize), Equals, offset)

	// Check 1Min at end of year
	t4 := time.Date(2018, time.December, 31, 23, 59, 0, 0, loc)
	index = TimeToIndex(t4, time.Minute)
	c.Assert(index, Equals, int64(525600))
	o_t4 := IndexToTime(index, time.Minute, 2018)
	c.Assert(o_t4, Equals, t4)

	offset = TimeToOffset(t4, time.Minute, recSize)
	o_o4 := IndexToOffset(index, recSize)
	c.Assert(o_o4, Equals, offset)

	epoch = t4.Unix()
	c.Assert(EpochToOffset(epoch, time.Minute, recSize), Equals, offset)
}

func (s *TestSuite) TestUnion(c *C) {
	csA := makeTestCS()
	csB := makeTestCS()

	// identical cs join
	cs := ColumnSeriesUnion(csA, csB)

	for name, col := range cs.GetColumns() {
		switch reflect.TypeOf(col).Kind() {
		case reflect.Slice:
			av := reflect.ValueOf(csA.columns[name])
			bv := reflect.ValueOf(csB.columns[name])
			cv := reflect.ValueOf(col)

			c.Assert(av.Len(), Equals, cv.Len())
			c.Assert(bv.Len(), Equals, cv.Len())

			for i := 0; i < cv.Len(); i++ {
				c.Assert(av.Index(i).Interface(), Equals, cv.Index(i).Interface())
				c.Assert(bv.Index(i).Interface(), Equals, cv.Index(i).Interface())
			}
		}
	}

	// shorter cs union
	c.Assert(csA.RestrictLength(2, LAST), IsNil)

	cs = ColumnSeriesUnion(csA, csB)

	c.Assert(len(cs.GetEpoch()), Equals, 3)
	c.Assert(cs.GetEpoch()[0], Equals, csB.GetEpoch()[0])
	c.Assert(cs.GetEpoch()[1], Equals, csA.GetEpoch()[0])
	c.Assert(cs.GetEpoch()[2], Equals, csA.GetEpoch()[1])

	// appending union
	col1 := []float32{4, 5, 6}
	col2 := []float64{4, 5, 6}
	col3 := []int32{4, 5, 6}
	col4 := []int64{4, 5, 6}
	col5 := []byte{4, 5, 6}
	csC := NewColumnSeries()
	csC.AddColumn("Epoch", col4)
	csC.AddColumn("One", col1)
	csC.AddColumn("Two", col2)
	csC.AddColumn("Three", col3)
	csC.AddColumn("Four", col4)
	csC.AddColumn("Five", col5)

	cs = ColumnSeriesUnion(csB, csC)
	c.Assert(len(cs.GetEpoch()), Equals, 6)
	c.Assert(cs.GetEpoch()[0], Equals, csB.GetEpoch()[0])
	c.Assert(cs.GetEpoch()[1], Equals, csB.GetEpoch()[1])
	c.Assert(cs.GetEpoch()[2], Equals, csB.GetEpoch()[2])
	c.Assert(cs.GetEpoch()[3], Equals, csC.GetEpoch()[0])
	c.Assert(cs.GetEpoch()[4], Equals, csC.GetEpoch()[1])
	c.Assert(cs.GetEpoch()[5], Equals, csC.GetEpoch()[2])
}

func (s *TestSuite) TestSliceByEpoch(c *C) {
	cs := makeTestCS()

	// just start
	start := int64(2)
	slc, err := SliceColumnSeriesByEpoch(*cs, &start, nil)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 2)
	c.Assert(slc.GetEpoch()[0], Equals, cs.GetEpoch()[1])

	// no slice
	start = int64(0)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, nil)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 3)
	c.Assert(slc.GetEpoch()[0], Equals, cs.GetEpoch()[0])

	// just end
	end := int64(3)
	slc, err = SliceColumnSeriesByEpoch(*cs, nil, &end)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 2)
	c.Assert(slc.GetEpoch()[1], Equals, cs.GetEpoch()[1])

	// no slice
	end = int64(4)
	slc, err = SliceColumnSeriesByEpoch(*cs, nil, &end)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 3)
	c.Assert(slc.GetEpoch()[2], Equals, cs.GetEpoch()[2])

	// start and end
	start = int64(2)
	end = int64(3)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, &end)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 1)
	c.Assert(slc.GetEpoch()[0], Equals, cs.GetEpoch()[1])

	// no slice
	start = int64(0)
	end = int64(4)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, &end)
	c.Assert(err, IsNil)
	c.Assert(slc, NotNil)
	c.Assert(slc.Len(), Equals, 3)
	c.Assert(slc.GetEpoch()[0], Equals, cs.GetEpoch()[0])
	c.Assert(slc.GetEpoch()[2], Equals, cs.GetEpoch()[2])
}

func (s *TestSuite) TestApplyTimeQual(c *C) {
	cs := makeTestCS()

	tq := func(epoch int64) bool {
		if epoch == int64(2) {
			return true
		}
		return false
	}

	tqCS := cs.ApplyTimeQual(tq)

	c.Assert(tqCS.Len(), Equals, 1)
	c.Assert(tqCS.GetEpoch()[0], Equals, cs.GetEpoch()[1])
	c.Assert(tqCS.GetByName("One").([]float32)[0], Equals, cs.GetByName("One").([]float32)[1])

	tq = func(epoch int64) bool {
		return false
	}

	c.Assert(cs.ApplyTimeQual(tq).Len(), Equals, 0)
}
