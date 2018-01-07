package io

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
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

type NumpyDataset struct {
	Header      []byte   `msgpack:"header"`
	ColumnNames []string `msgpack:"columnnames"`
	ColumnData  [][]byte `msgpack:"columndata"`
	/*
		These two fields aren't exported, so are used only in the build and extension
	*/
	length     int
	dataShapes []DataShape
}

func NewNumpyDataset(cs *ColumnSeries) (nds *NumpyDataset, err error) {
	nds = new(NumpyDataset)
	nds.length = cs.Len()
	nds.dataShapes = cs.GetDataShapes()
	nds.updateHeader()
	if err != nil {
		return nil, err
	}
	for i, name := range cs.GetColumnNames() {
		nds.ColumnNames = append(nds.ColumnNames, name)
		nds.ColumnData = append(nds.ColumnData, []byte{})
		i_col := cs.GetColumn(name)
		switch col := i_col.(type) {
		case []int8:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		case []int16:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		case []int32:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		case []int64:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		case []float32:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		case []float64:
			byteData, _ := Serialize(nil, col)
			nds.ColumnData[i] = append(nds.ColumnData[i], byteData...)
		default:
			return nil, errors.New("Unknown type when converting colseries to npd")
		}
	}
	return nds, nil
}

func (nds *NumpyDataset) updateHeader() (err error) {
	/*
		This uses "dataShapes" inside the nds to compose the header.
		If this nds was transferred from somewhere else without the
		nds.dataShapes being filled in, we would have a problem, so
		we catch that condition and throw an error.
	*/
	if nds.dataShapes == nil {
		return fmt.Errorf("dataShapes is empty, must have a valid dataShapes slice to proceed")
	}
	writer := &bytes.Buffer{}
	/*
		Preamble
	*/
	writer.Write([]byte("\x93NUMPY"))
	binary.Write(writer, binary.LittleEndian, uint8(1))
	binary.Write(writer, binary.LittleEndian, uint8(0))

	/*
		ColumnShapes, including overall length
	*/
	columnShapeSection := "["
	for _, shape := range nds.dataShapes {
		name := shape.Name
		typ := shape.Type
		if s, ok := typeMap[typ]; ok {
			columnShapeSection += fmt.Sprintf("('%v', '<%v', (%v,)), ", name, s, nds.length)
		} else {
			return fmt.Errorf("unable to map type, have: %v", typ)
		}
	}
	columnShapeSection += "]"

	/*
		Complete Header
		Hardwired to a single dimensional array
	*/
	shapeString := "(1,)"
	header := fmt.Sprintf("{'descr': %s, 'fortran_order': False, 'shape': %s,}", columnShapeSection, shapeString)
	pad := 16 - ((10 + len(header)) % 16)
	if pad > 0 {
		header += strings.Repeat(" ", pad)
	}

	binary.Write(writer, binary.LittleEndian, uint16(len(header)))
	writer.Write([]byte(header))
	nds.Header = writer.Bytes()
	return nil
}

func (nds *NumpyDataset) Len() int {
	return nds.length
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
		nds.dataShapes, nds.length, err = GetDataShapesFromNumpyHeader(nds.Header)
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
			Header:      nds.Header,
			ColumnNames: nds.ColumnNames,
			ColumnData:  nds.ColumnData,
			dataShapes:  nds.dataShapes,
			length:      nds.length,
		},
	}
	nmds.StartIndex = make(map[string]int)
	nmds.Lengths = make(map[string]int)
	nmds.StartIndex[tbk.String()] = 0
	nmds.Lengths[tbk.String()] = nds.length
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
	nmds.StartIndex[tbk.String()] = nmds.length
	nmds.Lengths[tbk.String()] = cs.Len()
	nmds.length += cs.Len()
	for idx, col := range colSeriesNames {
		newBuffer := SwapSliceData(cs.GetColumn(col), byte(0)).([]byte)
		nmds.ColumnData[idx] = append(nmds.ColumnData[idx], newBuffer...)
	}
	nmds.updateHeader()
	return nil
}

/*
Utility Functions
*/

type AlphaString string

func (a *AlphaString) Scan(state fmt.ScanState, verb rune) error {
	token, err := state.Token(true, unicode.IsLetter)
	if err != nil {
		if err != nil {
			return err
		}
	}
	*a = AlphaString(token)
	return nil
}

func GetDataShapesFromNumpyHeader(header []byte) (dsv []DataShape, length int, err error) {
	/*
		This is completely untested - left here just in case...
	*/
	var names []string
	var etypes []EnumElementType

	start := bytes.Index(header, []byte("["))
	end := bytes.Index(header, []byte("]"))
	if start == -1 || end == -1 {
		return nil, 0, errors.New("incorrectly formatted Numpy header")
	}
	tgt := strings.Trim(string(header[start+1:end-1]), " ")
	typeStrings := strings.Split(tgt, "),")
	for _, typeStr := range typeStrings {
		if len(typeStr) == 0 {
			break
		}
		typeStr := strings.Trim(typeStr, " ")
		var name string
		var dtype string
		var lengthStr string
		name = strings.Split(typeStr, ",")[0]
		dtype = strings.Split(typeStr, ",")[1]
		lengthStr = strings.Split(typeStr, ",")[2]
		name = strings.Trim(name, "( ' ")
		dtype = strings.Trim(dtype, "' <")
		lengthStr = strings.Trim(lengthStr, "( , )")
		length, err = strconv.Atoi(lengthStr)
		if err != nil {
			return nil, 0, fmt.Errorf("unable to parse header, token:%s, err:%s",
				typeStr, err.Error())
		}
		names = append(names, string(name))
		for key, el := range typeMap {
			if strings.EqualFold(dtype, el) {
				etypes = append(etypes, key)
				break
			}
		}
		if len(names) != len(etypes) {
			return nil, 0, fmt.Errorf("unable to map datatype")
		}
	}
	return NewDataShapeVector(names, etypes), length, nil
}
