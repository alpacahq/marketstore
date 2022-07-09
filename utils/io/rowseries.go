package io

import (
	"errors"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type RowsInterface interface {
	GetRow(i int) []byte // Position to the i-th record
	GetData() []byte     // Pointer to the beginning of the data
	GetNumRows() int
	GetRowLen() int
	SetRowLen(int)
}

type RowSeriesInterface interface {
	GetMetadataKey() string // The filesystem metadata key for this data
}

type Rows struct {
	ColumnInterface
	RowsInterface
	dataShape []DataShape
	data      []byte
	rowLen    int // We allow for a rowLen that might differ from the sum of dataShape for alignment, etc
}

func NewRows(dataShape []DataShape, data []byte) *Rows {
	return &Rows{dataShape: dataShape, data: data, rowLen: 0}
}

func (rows *Rows) GetColumn(colname string) (col interface{}) {
	var offset int
	for _, ds := range rows.GetDataShapes() {
		if ds.Name == colname {
			switch ds.Type {
			case FLOAT32:
				return getFloat32Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case FLOAT64:
				return getFloat64Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case INT16:
				return getInt16Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case INT32:
				return getInt32Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case INT64:
				return getInt64Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case UINT8:
				return getUInt8Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case UINT16:
				return getUInt16Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case UINT32:
				return getUInt32Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case UINT64:
				return getUInt64Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case STRING16:
				return getString16Column(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			case BOOL, BYTE:
				return getByteColumn(offset, rows.GetRowLen(), rows.GetNumRows(), rows.GetData())
			default:
				log.Error("unexpected column type specified:", ds.Type)
			}
		} else {
			offset += ds.Type.Size()
		}
	}
	return nil
}

func (rows *Rows) GetDataShapes() []DataShape {
	return rows.dataShape
}

func (rows *Rows) Len() int {
	return rows.GetNumRows()
}

func (rows *Rows) GetTime() ([]time.Time, error) {
	ep, ok := rows.GetColumn("Epoch").([]int64)
	if !ok {
		return nil, fmt.Errorf("cast epoch column to []int64: %v", rows.GetColumn("Epoch"))
	}
	ts := make([]time.Time, len(ep))
	nsi := rows.GetColumn("Nanoseconds")
	if nsi == nil {
		for i, secs := range ep {
			ts[i] = ToSystemTimezone(time.Unix(secs, 0))
		}
	} else {
		ns, ok := nsi.([]int32)
		if !ok {
			return nil, errors.New("parse Nanoseconds column to int32")
		}
		for i, secs := range ep {
			ts[i] = ToSystemTimezone(time.Unix(secs, int64(ns[i])))
		}
	}
	return ts, nil
}

func (rows *Rows) SetRowLen(rowLen int) {
	/*
		Call this to set a custom row length for this group of rows. This is needed when padding for alignment.
	*/
	if rowLen < rows.GetRowLen() {
		rowLen = rows.GetRowLen() // Make sure the requested rowLen is sane
	}
	rows.rowLen = rowLen
}

func (rows *Rows) GetRowLen() (rowLength int) {
	/*
		rowLen can be set directly to allow for alignment, etc, or this will set it based on sum of DataShape
	*/
	if rows.rowLen == 0 {
		for _, shape := range rows.dataShape {
			rows.rowLen += shape.Type.Size()
		}
	}
	return rows.rowLen
}

func (rows *Rows) GetNumRows() int {
	mylen := rows.GetRowLen()
	if mylen == 0 || len(rows.data) == 0 {
		return 0
	}
	return len(rows.data) / mylen
}

func (rows *Rows) GetData() []byte {
	return rows.data
}

func (rows *Rows) GetRow(i int) []byte {
	rowLen := rows.GetRowLen()
	start := i * rowLen
	end := start + rowLen
	return rows.data[start:end]
}

func (rows *Rows) ToColumnSeries() (*ColumnSeries, error) {
	cs := NewColumnSeries()
	int64Epochs, ok := rows.GetColumn("Epoch").([]int64)
	if !ok {
		return nil, errors.New("failed to cast epoch column to []int64")
	}
	cs.AddColumn("Epoch", int64Epochs)
	for _, ds := range rows.GetDataShapes() {
		if ds.Name == "Epoch" {
			continue
		}
		cs.AddColumn(ds.Name, rows.GetColumn(ds.Name))
	}
	return cs, nil
}

type RowSeries struct {
	RowSeriesInterface
	RowsInterface
	ColumnInterface
	rows        *Rows
	metadataKey TimeBucketKey
}

func NewRowSeries(
	key TimeBucketKey,
	data []byte,
	dataShape []DataShape,
	rowLen int,
	rowType EnumRecordType,
) *RowSeries {
	/*
		We have to add a column named _nanoseconds_ to the datashapes for a variable record type
		This is true because the read() function for variable types inserts a 32-bit nanoseconds column
	*/
	if rowType == VARIABLE {
		dataShape = append(dataShape, DataShape{"Nanoseconds", INT32})
	}
	rows := NewRows(dataShape, data)
	rows.SetRowLen(rowLen)
	return &RowSeries{
		metadataKey: key,
		rows:        rows,
	}
}

func (rs *RowSeries) GetMetadataKey() TimeBucketKey {
	return rs.metadataKey
}

func (rs *RowSeries) GetRow(i int) []byte {
	return rs.rows.GetRow(i)
}

func (rs *RowSeries) GetData() []byte {
	return rs.rows.GetData()
}

func (rs *RowSeries) GetNumRows() int {
	return rs.rows.GetNumRows()
}

func (rs *RowSeries) GetRowLen() int {
	return rs.rows.GetRowLen()
}

func (rs *RowSeries) SetRowLen(rowLen int) {
	rs.rows.SetRowLen(rowLen)
}

func (rs *RowSeries) GetColumn(colname string) (col interface{}) {
	return rs.rows.GetColumn(colname)
}

func (rs *RowSeries) GetDataShapes() (ds []DataShape) {
	return rs.rows.GetDataShapes()
}

func (rs *RowSeries) Len() int {
	return rs.GetNumRows()
}

func (rs *RowSeries) GetTime() ([]time.Time, error) {
	return rs.rows.GetTime()
}

func (rs *RowSeries) GetEpoch() (col []int64) {
	return getInt64Column(0, rs.GetRowLen(), rs.GetNumRows(), rs.GetData())
}

func (rs *RowSeries) ToColumnSeries() (key TimeBucketKey, cs *ColumnSeries) {
	key = rs.GetMetadataKey()
	cs = NewColumnSeries()
	cs.AddColumn("Epoch", rs.GetEpoch())
	for _, ds := range rs.rows.GetDataShapes() {
		if ds.Name == "Epoch" {
			continue
		}
		cs.AddColumn(ds.Name, rs.GetColumn(ds.Name))
	}
	return key, cs
}
