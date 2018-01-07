package io

import (
	"time"
)

type RowsInterface interface {
	SetCandleAttributes(*CandleAttributes)
	GetCandleAttributes() *CandleAttributes
	GetRow(i int) []byte // Position to the i-th record
	GetData() []byte     // Pointer to the beginning of the data
	GetNumRows() int
	GetRowLen() int
	SetRowLen(int)
}

type RowSeriesInterface interface {
	GetMetadataKey() string // The filesystem metadata key for this data
	GetTPrev() time.Time    // The first timestamp of data just prior to the first row
}

type Rows struct {
	ColumnInterface
	RowsInterface
	dataShape        []DataShape
	data             []byte
	rowLen           int               // We allow for a rowLen that might differ from the sum of dataShape for alignment, etc
	candleAttributes *CandleAttributes // Attributes of the rows, are they discrete (ticks) or continuous (candles)
}

func NewRows(dataShape []DataShape, data []byte) *Rows {
	ca := CandleAttributes(0)
	return &Rows{dataShape: dataShape, data: data, rowLen: 0, candleAttributes: &ca}
}

func (rows *Rows) GetColumn(colname string) (col interface{}) {
	var offset int
	for _, ds := range rows.GetDataShapes() {
		if ds.Name == colname {
			switch ds.Type {
			case FLOAT32:
				return getFloat32Column(offset, int(rows.GetRowLen()), rows.GetNumRows(), rows.GetData())
			case FLOAT64:
				return getFloat64Column(offset, int(rows.GetRowLen()), rows.GetNumRows(), rows.GetData())
			case INT32:
				return getInt32Column(offset, int(rows.GetRowLen()), rows.GetNumRows(), rows.GetData())
			case EPOCH, INT64:
				return getInt64Column(offset, int(rows.GetRowLen()), rows.GetNumRows(), rows.GetData())
			case BOOL:
				fallthrough
			case BYTE:
				return getByteColumn(offset, int(rows.GetRowLen()), rows.GetNumRows(), rows.GetData())
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
func (rows *Rows) GetTime() []time.Time {
	ep := rows.GetColumn("Epoch").([]int64)
	ts := make([]time.Time, len(ep))
	nsi := rows.GetColumn("Nanoseconds")
	if nsi == nil {
		for i, secs := range ep {
			ts[i] = time.Unix(secs, 0).UTC()
		}
	} else {
		ns := nsi.([]int32)
		for i, secs := range ep {
			ts[i] = time.Unix(secs, int64(ns[i])).UTC()
		}
	}
	return ts
}

func (rows *Rows) SetCandleAttributes(ca *CandleAttributes) {
	rows.candleAttributes = ca
}

func (rows *Rows) GetCandleAttributes() *CandleAttributes {
	return rows.candleAttributes
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
func (rows *Rows) GetRowLen() (len int) {
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
func (rows *Rows) ToColumnSeries() *ColumnSeries {
	cs := NewColumnSeries()
	cs.AddColumn("Epoch", rows.GetColumn("Epoch").([]int64))
	for _, ds := range rows.GetDataShapes() {
		if ds.Name == "Epoch" {
			continue
		}
		cs.AddColumn(ds.Name, rows.GetColumn(ds.Name))
	}
	cs.SetCandleAttributes(rows.GetCandleAttributes())
	return cs
}

type RowSeries struct {
	RowSeriesInterface
	RowsInterface
	ColumnInterface
	rows        *Rows
	metadataKey TimeBucketKey
	tPrev       time.Time
}

func NewRowSeries(key TimeBucketKey, tPrev int64, data []byte, dataShape []DataShape, rowLen int, cat *CandleAttributes,
	rowType EnumRecordType) *RowSeries {
	/*
		We have to add a column named _nanoseconds_ to the datashapes for a variable record type
		This is true because the read() function for variable types inserts a 32-bit nanoseconds column
	*/
	if rowType == VARIABLE {
		dataShape = append(dataShape, DataShape{"Nanoseconds", INT32})
	}
	timePrev := time.Unix(tPrev, 0).UTC()
	rows := NewRows(dataShape, data)
	rows.SetCandleAttributes(cat)
	rows.SetRowLen(rowLen)
	return &RowSeries{
		metadataKey: key,
		tPrev:       timePrev,
		rows:        rows,
	}
}

func (rs *RowSeries) GetMetadataKey() TimeBucketKey {
	return rs.metadataKey
}
func (rs *RowSeries) GetTPrev() time.Time {
	return rs.tPrev
}

func (rs *RowSeries) SetCandleAttributes(ca *CandleAttributes) {
	rs.rows.SetCandleAttributes(ca)
}
func (rs *RowSeries) GetCandleAttributes() *CandleAttributes {
	return rs.rows.GetCandleAttributes()
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
func (rs *RowSeries) GetTime() []time.Time {
	return rs.rows.GetTime()
}

func (rs *RowSeries) GetEpoch() (col []int64) {
	return getInt64Column(0, int(rs.GetRowLen()), rs.GetNumRows(), rs.GetData())
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
	cs.SetCandleAttributes(rs.GetCandleAttributes())
	return key, cs
}
