package io

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNumpyDataset(t *testing.T) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	assert.Nil(t, err)
	assert.Equal(t, nds.Length, 3)
	assert.Equal(t, nds.ColumnNames[0], "Epoch")
	assert.Len(t, nds.ColumnData, 1)
	assert.Equal(t, nds.Length, 3)

	dsv, err := nds.buildDataShapes()
	assert.Len(t, dsv, 1)
	assert.Nil(t, err)
}

func TestNewNumpyMultiDataset(t *testing.T) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	assert.Nil(t, err)
	tbk := NewTimeBucketKey("TSLA/1Min/OHLCV")
	nmds, err := NewNumpyMultiDataset(nds, *tbk)
	assert.Nil(t, err)
	assert.Equal(t, nmds.Length, 3)
	assert.Equal(t, nmds.ColumnNames[0], "Epoch")
	assert.Equal(t, nmds.StartIndex[tbk.String()], 0)
	assert.Len(t, nmds.ColumnData, 1)
	assert.Equal(t, nmds.Length, 3)
}

func TestAppend(t *testing.T) {
	epoch := []int64{10, 11, 12}
	col1 := []float32{5.5, 6.6, 7.7}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("col1", col1)
	nds, err := NewNumpyDataset(cs)
	assert.Nil(t, err)
	tbk := NewTimeBucketKey("TSLA/1Min/OHLCV")
	nmds, err := NewNumpyMultiDataset(nds, *tbk)
	assert.Nil(t, err)
	assert.Equal(t, nmds.Length, 3)
	assert.Equal(t, nmds.ColumnNames[0], "Epoch")
	assert.Equal(t, nmds.ColumnNames[1], "col1")
	assert.Equal(t, nmds.StartIndex[tbk.String()], 0)
	assert.Len(t, nmds.ColumnData, 2)
	assert.Equal(t, nmds.Length, 3)

	epoch = []int64{5, 6, 7}
	col3 := []float32{1.1, 2.2, 3.3}
	cs2 := NewColumnSeries()
	cs2.AddColumn("Epoch", epoch)
	cs2.AddColumn("col1", col3)
	tbk2 := NewTimeBucketKey("NVDA/1Min/OHLCV")
	err = nmds.Append(cs2, *tbk2)
	assert.Nil(t, err)
	assert.Equal(t, nmds.Lengths[tbk2.String()], 3)
	assert.Equal(t, nmds.Length, 6)
	assert.Len(t, nmds.ColumnData, 2)
	badCol := []int64{12, 13, 14, 15}
	badCS := NewColumnSeries()
	badCS.AddColumn("bad", badCol)
	tbkBad := NewTimeBucketKey("FORD/1Min/OHLCV")
	err = nmds.Append(badCS, *tbkBad)
	assert.NotNil(t, err)
}

func TestToColumnSeries(t *testing.T) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	assert.Nil(t, err)
	assert.Equal(t, nds.Length, 3)
	assert.Equal(t, nds.ColumnNames[0], "Epoch")
	assert.Len(t, nds.ColumnData[0], 24)
	assert.Equal(t, nds.Len(), 3)

	csReturned, err := nds.ToColumnSeries(0, nds.Len())
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(csReturned, cs))

	nds.dataShapes = nil
	csReturned, err = nds.ToColumnSeries(0, cs.Len())
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(csReturned, cs))
}
