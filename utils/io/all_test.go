package io

import (
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	. "gopkg.in/check.v1"

	"github.com/alpacahq/marketstore/v4/utils"
)

type ohlc struct {
	index      int64
	o, h, l, c float32
}

func TestConvertByteSlice(t *testing.T) {
	t.Parallel()
	s1 := ohlc{100000000, 200, 300, 400, 500}
	s2 := ohlc{10, 20, 30, 40, 50}
	baseArray := [2]ohlc{s1, s2}
	buffer := make([]byte, 48)
	copy(buffer, (*(*[24]byte)(unsafe.Pointer(&s1)))[:])
	copy(buffer[24:], (*(*[24]byte)(unsafe.Pointer(&s2)))[:])

	i_myarray := CopySliceByte(buffer, ohlc{})
	myarray := i_myarray.([]ohlc)
	for i, ohlc := range myarray {
		assert.Equal(t, ohlc, baseArray[i])
	}

	i_myarray = SwapSliceByte(buffer, ohlc{})
	myarray = i_myarray.([]ohlc)
	for i, ohlc := range myarray {
		assert.Equal(t, ohlc, baseArray[i])
	}

	i_mybyte := SwapSliceData(myarray, byte(0))
	bytearray := i_mybyte.([]byte)
	assert.Len(t, bytearray, 48)
	for i, val := range bytearray {
		assert.Equal(t, val, buffer[i])
	}

	bytearray = CastToByteSlice(myarray)
	assert.Len(t, bytearray, 48)
	for i, val := range bytearray {
		assert.Equal(t, val, buffer[i])
	}

	i_myarray = SwapSliceData(buffer, ohlc{})
	myarray = i_myarray.([]ohlc)
	assert.Len(t, myarray, 2)
	for i, val := range myarray {
		assert.Equal(t, val, baseArray[i])
	}

	myInt64 := int64(65793)
	bs := DataToByteSlice(myInt64)
	assert.Len(t, bs, 8)
	for i, val := range []byte{1, 1, 1, 0, 0, 0, 0, 0} {
		assert.Equal(t, bs[i], val)
	}
}

func TestVariableBoundaryCases(t *testing.T) {
	t.Parallel()
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
	// fmt.Println("Test Time:  ", t1, " Minutes: ", t1.Minute(), " Seconds: ", t1.Second())

	// Check the 1Min interval
	index := TimeToIndex(t1, time.Minute)
	o_t1 := IndexToTime(index, time.Minute, 2008)
	// fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	assert.Equal(t, o_t1.Minute(), 24)
	assert.Equal(t, o_t1.Second(), 0)
	ticks := GetIntervalTicks32Bit(t1, index, 1440)
	seconds := t1.Second()
	nanos := t1.Nanosecond()
	fractionalSeconds := float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval := fractionalSeconds / 60.
	intervalTicks := uint32(fractionalInterval * math.MaxUint32)
	// fmt.Println("Interval Ticks: ", intervalTicks)
	assert.Equal(t, intervalTicks, ticks)

	t1 = time.Date(2008, time.January, 3, 16, 24, 59, 1000*839106, time.UTC)

	index = TimeToIndex(t1, time.Minute)
	o_t1 = IndexToTime(index, time.Minute, 2008)
	// fmt.Println("Index Time: ", o_t1, " Minutes: ", o_t1.Minute(), " Seconds: ", o_t1.Second())
	assert.Equal(t, o_t1.Minute(), 24)
	assert.Equal(t, o_t1.Second(), 0)
	ticks = GetIntervalTicks32Bit(t1, index, 1440)
	seconds = t1.Second()
	nanos = t1.Nanosecond()
	fractionalSeconds = float64(seconds) + float64(nanos)/1000000000.
	fractionalInterval = fractionalSeconds / 60.
	intervalTicks = uint32(fractionalInterval * math.MaxUint32)
	// fmt.Println("Interval Ticks: ", intervalTicks, " Ticks: ", ticks)
	diff := int64(intervalTicks) - int64(ticks)
	if diff < 0 {
		diff = -diff
	}
	assert.True(t, diff < 2)
}

func TestGenerics(t *testing.T) {
	t.Parallel()
	// DownSizeSlice
	input := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	output, err := DownSizeSlice(input, 6, LAST)
	expected := []float64{5, 6, 7, 8, 9, 10}
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(output, expected))

	output, err = DownSizeSlice(input, 6, FIRST)
	expected = []float64{1, 2, 3, 4, 5, 6}
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(output, expected), Equals)

	input2 := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	output2, err := DownSizeSlice(input2, 6, FIRST)
	expected2 := []uint32{1, 2, 3, 4, 5, 6}
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(output2, expected2))

	input3 := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	output3, err := DownSizeSlice(input3, 6, FIRST)
	expected3 := []string{"1", "2", "3", "4", "5", "6"}
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(output3, expected3))

	/*
		Test the error case
	*/
	_, err = DownSizeSlice("Should not work", 100, FIRST)
	assert.NotNil(t, err)
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

func TestSerializeColumnsToRows(t *testing.T) {
	t.Parallel()
	csA := makeTestCS()
	assert.Equal(t, csA.Len(), 3)

	dsv := csA.GetDataShapes()

	/*
		Unaligned case
	*/
	UnalignedBytesPerRow := 8 + 4 + 8 + 4 + 8 + 1
	data, reclen, err := SerializeColumnsToRows(csA, dsv, false)
	assert.Nil(t, err)
	assert.True(t, reclen == UnalignedBytesPerRow)
	assert.True(t, reclen*3 == len(data))

	/*
		Aligned case
	*/
	AlignedBytesPerRow := AlignedSize(UnalignedBytesPerRow)
	data, reclen, err = SerializeColumnsToRows(csA, dsv, true)
	assert.Nil(t, err)
	assert.True(t, reclen == AlignedBytesPerRow)
	assert.True(t, reclen*3 == len(data))

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

	data, reclen, err = SerializeColumnsToRows(csA, dsvProjected, true)
	assert.Nil(t, err)
	assert.True(t, reclen == expectedLen)
	assert.True(t, reclen*3 == len(data))
	/*
		Type Coercion case
	*/
	err = csB.CoerceColumnType("Two", BYTE) // Is currently FLOAT64
	assert.Nil(t, err)
	dsvProjected = csB.GetDataShapes()

	// Expected record length
	expectedLen = 0
	for _, shape := range dsvProjected {
		expectedLen += shape.Len()
	}
	expectedLen = AlignedSize(expectedLen)

	data, reclen, err = SerializeColumnsToRows(csA, dsvProjected, true)
	assert.Nil(t, err)
	assert.True(t, reclen == expectedLen)
	assert.True(t, reclen*3 == len(data))
}

func TestTimeBucketInfo(t *testing.T) {
	t.Parallel()
	tempDir, _ := ioutil.TempDir("", "io.TestTimeBucketInfo")
	defer os.RemoveAll(tempDir)

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
	assert.Nil(t, err)
	WriteHeader(testFilePath, tbi)

	tbi2 := TimeBucketInfo{
		Year: year,
		Path: filePath,
	}

	nElements := tbi2.GetNelements()
	assert.Equal(t, nElements, int32(2))

	assert.Equal(t, tbi2.GetVariableRecordLength(), int32(0))

	fcopy := tbi2.GetDeepCopy()
	assert.Equal(t, fcopy.timeframe.Nanoseconds(), tbi.timeframe.Nanoseconds())

	dsv2 := tbi2.GetDataShapes()
	assert.Equal(t, dsv2[0].String(), dsv[0].String())
	assert.Equal(t, dsv2[0].Equal(dsv[0]), true)
}

func TestIndexAndOffset(t *testing.T) {
	t.Parallel()
	recSize := int32(28)
	loc, _ := time.LoadLocation("America/New_York")
	utils.InstanceConfig.Timezone = loc

	// check the 1Min interval
	t0 := time.Date(2018, time.January, 1, 0, 0, 0, 0, loc)
	index := TimeToIndex(t0, time.Minute)
	assert.Equal(t, index, int64(1))
	o_t0 := IndexToTime(index, time.Minute, 2018)
	assert.Equal(t, o_t0, t0)

	offset := TimeToOffset(t0, time.Minute, recSize)
	o_o0 := IndexToOffset(index, recSize)
	assert.Equal(t, o_o0, offset)

	epoch := t0.Unix()
	assert.Equal(t, EpochToOffset(epoch, time.Minute, recSize), offset)

	// Check the 5Min interval
	t1 := time.Date(2018, time.January, 1, 0, 5, 0, 0, loc)
	index = TimeToIndex(t1, 5*time.Minute)
	assert.Equal(t, index, int64(2))
	o_t1 := IndexToTime(index, 5*time.Minute, 2018)
	assert.Equal(t, o_t1, t1)

	offset = TimeToOffset(t1, 5*time.Minute, recSize)
	o_o1 := IndexToOffset(index, recSize)
	assert.Equal(t, o_o1, offset)

	epoch = t1.Unix()
	assert.Equal(t, EpochToOffset(epoch, 5*time.Minute, recSize), offset)

	// Check the 1D interval
	t2 := time.Date(2018, time.February, 5, 0, 0, 0, 0, loc)
	index = TimeToIndex(t2, utils.Day)
	assert.Equal(t, index, int64(35))
	o_t2 := IndexToTime(index, utils.Day, 2018)
	assert.Equal(t, o_t2 == t2, true)

	offset = TimeToOffset(t2, utils.Day, recSize)
	o_o2 := IndexToOffset(index, recSize)
	assert.Equal(t, o_o2, offset)

	epoch = t2.Unix()
	assert.Equal(t, EpochToOffset(epoch, utils.Day, recSize), offset)

	// Check 1D at end of year
	t3 := time.Date(2018, time.December, 31, 0, 0, 0, 0, loc)
	index = TimeToIndex(t3, utils.Day)
	assert.Equal(t, index, int64(364))
	o_t3 := IndexToTime(index, utils.Day, 2018)
	assert.Equal(t, o_t3, t3)

	offset = TimeToOffset(t3, utils.Day, recSize)
	o_o3 := IndexToOffset(index, recSize)
	assert.Equal(t, o_o3, offset)

	epoch = t3.Unix()
	assert.Equal(t, EpochToOffset(epoch, utils.Day, recSize), offset)

	// Check 1Min at end of year
	t4 := time.Date(2018, time.December, 31, 23, 59, 0, 0, loc)
	index = TimeToIndex(t4, time.Minute)
	assert.Equal(t, index, int64(525600))
	o_t4 := IndexToTime(index, time.Minute, 2018)
	assert.Equal(t, o_t4, t4)

	offset = TimeToOffset(t4, time.Minute, recSize)
	o_o4 := IndexToOffset(index, recSize)
	assert.Equal(t, o_o4, offset)

	epoch = t4.Unix()
	assert.Equal(t, EpochToOffset(epoch, time.Minute, recSize), offset)
}

func TestUnion(t *testing.T) {
	t.Parallel()
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

			assert.Equal(t, av.Len(), cv.Len())
			assert.Equal(t, bv.Len(), cv.Len())

			for i := 0; i < cv.Len(); i++ {
				assert.Equal(t, av.Index(i).Interface(), cv.Index(i).Interface())
				assert.Equal(t, bv.Index(i).Interface(), cv.Index(i).Interface())
			}
		}
	}

	// shorter cs union
	assert.Nil(t, csA.RestrictLength(2, LAST))

	cs = ColumnSeriesUnion(csA, csB)

	assert.Equal(t, len(cs.GetEpoch()), 3)
	assert.Equal(t, cs.GetEpoch()[0], csB.GetEpoch()[0])
	assert.Equal(t, cs.GetEpoch()[1], csA.GetEpoch()[0])
	assert.Equal(t, cs.GetEpoch()[2], csA.GetEpoch()[1])

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
	assert.Equal(t, len(cs.GetEpoch()), 6)
	assert.Equal(t, cs.GetEpoch()[0], csB.GetEpoch()[0])
	assert.Equal(t, cs.GetEpoch()[1], csB.GetEpoch()[1])
	assert.Equal(t, cs.GetEpoch()[2], csB.GetEpoch()[2])
	assert.Equal(t, cs.GetEpoch()[3], csC.GetEpoch()[0])
	assert.Equal(t, cs.GetEpoch()[4], csC.GetEpoch()[1])
	assert.Equal(t, cs.GetEpoch()[5], csC.GetEpoch()[2])
}

func TestSliceByEpoch(t *testing.T) {
	t.Parallel()
	cs := makeTestCS()

	// just start
	start := int64(2)
	slc, err := SliceColumnSeriesByEpoch(*cs, &start, nil)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 2)
	assert.Equal(t, slc.GetEpoch()[0], cs.GetEpoch()[1])

	// no slice
	start = int64(0)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, nil)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 3)
	assert.Equal(t, slc.GetEpoch()[0], cs.GetEpoch()[0])

	// just end
	end := int64(3)
	slc, err = SliceColumnSeriesByEpoch(*cs, nil, &end)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 2)
	assert.Equal(t, slc.GetEpoch()[1], cs.GetEpoch()[1])

	// no slice
	end = int64(4)
	slc, err = SliceColumnSeriesByEpoch(*cs, nil, &end)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 3)
	assert.Equal(t, slc.GetEpoch()[2], cs.GetEpoch()[2])

	// start and end
	start = int64(2)
	end = int64(3)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, &end)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 1)
	assert.Equal(t, slc.GetEpoch()[0], cs.GetEpoch()[1])

	// no slice
	start = int64(0)
	end = int64(4)
	slc, err = SliceColumnSeriesByEpoch(*cs, &start, &end)
	assert.Nil(t, err)
	assert.NotNil(t, slc)
	assert.Equal(t, slc.Len(), 3)
	assert.Equal(t, slc.GetEpoch()[0], cs.GetEpoch()[0])
	assert.Equal(t, slc.GetEpoch()[2], cs.GetEpoch()[2])
}

func TestApplyTimeQual(t *testing.T) {
	t.Parallel()
	cs := makeTestCS()

	tq := func(epoch int64) bool {
		return epoch == int64(2)
	}

	tqCS := cs.ApplyTimeQual(tq)

	assert.Equal(t, tqCS.Len(), 1)
	assert.Equal(t, tqCS.GetEpoch()[0], cs.GetEpoch()[1])
	assert.Equal(t, tqCS.GetByName("One").([]float32)[0], cs.GetByName("One").([]float32)[1])

	tq = func(epoch int64) bool {
		return false
	}

	assert.Equal(t, cs.ApplyTimeQual(tq).Len(), 0)
}
