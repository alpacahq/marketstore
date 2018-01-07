package io

import (
	"reflect"

	. "gopkg.in/check.v1"
)

type TestSuite3 struct{}

var _ = Suite(&TestSuite3{})

func (s *TestSuite3) TestNewNumpyDataset(c *C) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	c.Check(err, Equals, nil)
	c.Check(nds.length, Equals, 3)
	c.Check(nds.ColumnNames[0], Equals, "Epoch")
	testHeader := "\x93NUMPY\x01\x00V\x00{'descr': [('Epoch', '<i8', (3,)), ], 'fortran_order': False, 'shape': (1,),}         "
	c.Check(string(nds.Header), Equals, string(testHeader))
	c.Check(len(nds.ColumnData), Equals, 1)
	c.Check(nds.length, Equals, 3)
}

func (s *TestSuite3) TestNewNumpyMultiDataset(c *C) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	c.Check(err, Equals, nil)
	tbk := NewTimeBucketKey("TSLA/1Min/OHLCV")
	nmds, err := NewNumpyMultiDataset(nds, *tbk)
	c.Check(err, Equals, nil)
	c.Check(nmds.length, Equals, 3)
	c.Check(nmds.ColumnNames[0], Equals, "Epoch")
	testHeader := "\x93NUMPY\x01\x00V\x00{'descr': [('Epoch', '<i8', (3,)), ], 'fortran_order': False, 'shape': (1,),}         "
	c.Check(string(nmds.Header), Equals, string(testHeader))
	c.Check(nmds.StartIndex[tbk.String()], Equals, 0)
	c.Check(len(nmds.ColumnData), Equals, 1)
	c.Check(nmds.length, Equals, 3)
}

func (s *TestSuite3) TestAppend(c *C) {
	epoch := []int64{10, 11, 12}
	col1 := []float32{5.5, 6.6, 7.7}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("col1", col1)
	nds, err := NewNumpyDataset(cs)
	c.Check(err, Equals, nil)
	tbk := NewTimeBucketKey("TSLA/1Min/OHLCV")
	nmds, err := NewNumpyMultiDataset(nds, *tbk)
	c.Check(err, Equals, nil)
	c.Check(nmds.length, Equals, 3)
	c.Check(nmds.ColumnNames[0], Equals, "Epoch")
	c.Check(nmds.ColumnNames[1], Equals, "col1")
	c.Check(nmds.StartIndex[tbk.String()], Equals, 0)
	c.Check(len(nmds.ColumnData), Equals, 2)
	c.Check(nmds.length, Equals, 3)

	epoch = []int64{5, 6, 7}
	col3 := []float32{1.1, 2.2, 3.3}
	cs2 := NewColumnSeries()
	cs2.AddColumn("Epoch", epoch)
	cs2.AddColumn("col1", col3)
	tbk2 := NewTimeBucketKey("NVDA/1Min/OHLCV")
	err = nmds.Append(cs2, *tbk2)
	c.Check(err, Equals, nil)
	c.Check(nmds.Lengths[tbk2.String()], Equals, 3)
	c.Check(nmds.length, Equals, 6)
	c.Check(len(nmds.ColumnData), Equals, 2)
	badCol := []int64{12, 13, 14, 15}
	badCS := NewColumnSeries()
	badCS.AddColumn("bad", badCol)
	tbkBad := NewTimeBucketKey("FORD/1Min/OHLCV")
	err = nmds.Append(badCS, *tbkBad)
	c.Check(err != nil, Equals, true)
}

func (s *TestSuite3) TestToColumnSeries(c *C) {
	epoch := []int64{10, 11, 12}
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	nds, err := NewNumpyDataset(cs)
	c.Check(err, Equals, nil)
	c.Check(nds.length, Equals, 3)
	c.Check(nds.ColumnNames[0], Equals, "Epoch")
	testHeader := "\x93NUMPY\x01\x00V\x00{'descr': [('Epoch', '<i8', (3,)), ], 'fortran_order': False, 'shape': (1,),}         "
	c.Check(string(nds.Header), Equals, string(testHeader))
	c.Check(len(nds.ColumnData[0]), Equals, 24)
	c.Check(nds.Len(), Equals, 3)

	csReturned, err := nds.ToColumnSeries(0, nds.Len())
	c.Check(err, Equals, nil)
	c.Check(reflect.DeepEqual(csReturned, cs), Equals, true)
}

func (s *TestSuite3) TestGetDataShapesFromNumpyHeader(c *C) {
	testHeader := []byte("[('Epoch', '<i8', (2,)), ('Open', '<f4', (2,)), ('High', '<f4', (2,)), ('Low', '<f4', (2,)), ('Close', '<f4', (2,)), ('Volume', '<i4', (2,)]")
	dsv, length, err := GetDataShapesFromNumpyHeader(testHeader)
	c.Check(length, Equals, 2)
	c.Check(err, Equals, nil)
	toTestDsv := ""
	for _, d := range dsv {
		toTestDsv += d.String()
	}
	checkDsv := "Epoch:INT64Open:FLOAT32High:FLOAT32Low:FLOAT32Close:FLOAT32Volume:INT32"
	c.Check(toTestDsv, Equals, checkDsv)
}
