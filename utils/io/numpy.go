package io

// TODO: this is no longer numpy.  rename later.
import (
	"errors"
	"fmt"

	"github.com/golang/glog"
)

var (
	typeMap = map[EnumElementType]string{
		BYTE:    "i1",
		INT16:   "i2",
		INT32:   "i4",
		INT64:   "i8",
		UINT8:   "u1",
		UINT16:  "u2",
		UINT32:  "u4",
		UINT64:  "u8",
		FLOAT32: "f4",
		FLOAT64: "f8",
	}
)

var typeStrMap = func() map[string]EnumElementType {
	m := map[string]EnumElementType{}
	for key, val := range typeMap {
		m[val] = key
	}
	return m
}()

type NumpyDataset struct {
	// a list of type strings such as i4 and f8
	ColumnTypes []string `msgpack:"types"`
	// a list of column names
	ColumnNames []string `msgpack:"names"`
	// two dimentional byte arrays holding the column data
	ColumnData [][]byte `msgpack:"data"`
	Length     int      `msgpack:"length"`
	// hidden
	dataShapes []DataShape
}

func NewNumpyDataset(cs *ColumnSeries) (nds *NumpyDataset, err error) {
	nds = new(NumpyDataset)
	nds.Length = cs.Len()
	nds.dataShapes = cs.GetDataShapes()
	for i, name := range cs.GetColumnNames() {
		nds.ColumnNames = append(nds.ColumnNames, name)
		colBytes := CastToByteSlice(cs.GetColumn(name))
		nds.ColumnData = append(nds.ColumnData, colBytes)
		if typeStr, ok := typeMap[nds.dataShapes[i].Type]; !ok {
			glog.Errorf("unsupported type %v", nds.dataShapes[i].String())
			return nil, fmt.Errorf("unsupported type")
		} else {
			nds.ColumnTypes = append(nds.ColumnTypes, typeStr)
		}
	}
	return nds, nil
}

func (nds *NumpyDataset) Len() int {
	return nds.Length
}

func (nds *NumpyDataset) buildDataShapes() ([]DataShape, error) {
	etypes := []EnumElementType{}
	for _, typeStr := range nds.ColumnTypes {
		if typ, ok := typeStrMap[typeStr]; !ok {
			return nil, fmt.Errorf("unsupported type string %s", typeStr)
		} else {
			etypes = append(etypes, typ)
		}
	}
	return NewDataShapeVector(nds.ColumnNames, etypes), nil
}

func (nds *NumpyDataset) ToColumnSeries(options ...int) (cs *ColumnSeries, err error) {
	var startIndex, length int
	if len(options) != 0 {
		if len(options) != 2 {
			return nil, fmt.Errorf("incorrect number of arguments")
		}
		startIndex, length = options[0], options[1]
	} else {
		startIndex, length = 0, nds.Len()
	}

	cs = NewColumnSeries()
	if len(nds.ColumnData[0]) == 0 {
		return cs, nil
	}
	/*
		Coerce the []byte for each column into it's native pointer type
	*/
	if nds.dataShapes == nil {
		nds.dataShapes, err = nds.buildDataShapes()
		if err != nil {
			return nil, err
		}
	}
	for i, shape := range nds.dataShapes {
		size := shape.Type.Size()
		start := startIndex * size
		end := start + length*size
		newColData := shape.Type.ConvertByteSliceInto(nds.ColumnData[i][start:end])
		cs.AddColumn(shape.Name, newColData)
	}
	return cs, nil
}

type NumpyMultiDataset struct {
	NumpyDataset
	StartIndex map[string]int `msgpack:"startindex"`
	Lengths    map[string]int `msgpack:"lengths"`
}

func NewNumpyMultiDataset(nds *NumpyDataset, tbk TimeBucketKey) (nmds *NumpyMultiDataset, err error) {
	nmds = &NumpyMultiDataset{
		NumpyDataset: NumpyDataset{
			ColumnTypes: nds.ColumnTypes,
			ColumnNames: nds.ColumnNames,
			ColumnData:  nds.ColumnData,
			Length:      nds.Length,
			dataShapes:  nds.dataShapes,
		},
	}
	nmds.StartIndex = make(map[string]int)
	nmds.Lengths = make(map[string]int)
	nmds.StartIndex[tbk.String()] = 0
	nmds.Lengths[tbk.String()] = nds.Length
	return nmds, nil
}

func (nmds *NumpyMultiDataset) ToColumnSeriesMap() (csm ColumnSeriesMap, err error) {
	csm = NewColumnSeriesMap()
	for tbkStr, idx := range nmds.StartIndex {
		length := nmds.Lengths[tbkStr]
		var cs *ColumnSeries
		if length > 0 {
			cs, err = nmds.ToColumnSeries(idx, length)
			if err != nil {
				return nil, err
			}
		} else {
			cs = NewColumnSeries()
		}
		tbk, _ := NewTimeBucketKeyFromString(tbkStr)
		csm.AddColumnSeries(*tbk, cs)
	}
	return csm, nil
}

func (nmds *NumpyMultiDataset) Append(cs *ColumnSeries, tbk TimeBucketKey) (err error) {
	if len(nmds.ColumnData) != cs.GetNumColumns() {
		err = errors.New("Length of columns mismatch with NumpyMultiDataset")
		return
	}
	colSeriesNames := cs.GetColumnNames()
	for idx, name := range nmds.ColumnNames {
		if name != colSeriesNames[idx] {
			err = errors.New("Data shape mismatch of ColumnSeries and NumpyMultiDataset")
			return
		}
	}
	nmds.StartIndex[tbk.String()] = nmds.Length
	nmds.Lengths[tbk.String()] = cs.Len()
	nmds.Length += cs.Len()
	for idx, col := range colSeriesNames {
		newBuffer := CastToByteSlice(cs.GetColumn(col))
		nmds.ColumnData[idx] = append(nmds.ColumnData[idx], newBuffer...)
	}
	return nil
}
