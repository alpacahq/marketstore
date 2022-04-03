package io

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type DataShape struct {
	Name string
	Type EnumElementType
}

// NewDataShapeVector returns a new array of DataShapes for the given array of
// names and element types.
func NewDataShapeVector(names []string, etypes []EnumElementType) (dsv []DataShape) {
	for i, name := range names {
		dsv = append(dsv, DataShape{name, etypes[i]})
	}
	return dsv
}

// Len returns the length of the DataShape.
func (ds *DataShape) Len() (out int) {
	return ds.Type.Size()
}

// String returns the colon-separated string of the DataShapes name and type.
func (ds *DataShape) String() (st string) {
	return ds.Name + ":" + ds.Type.String()
}

// Equal compares both the name and type of two DataShapes, only returning true
// if both are equal.
func (ds *DataShape) Equal(shape DataShape) bool {
	return ds.Name == shape.Name && ds.Type == shape.Type
}

func DataShapesFromInputString(inputStr string) (dsa []DataShape, err error) {
	splitString := strings.Split(inputStr, ":")
	dsa = make([]DataShape, 0)
	for _, group := range splitString {
		twoParts := strings.Split(group, "/")
		if len(twoParts) != 2 {
			err = fmt.Errorf("error: %s: Data shape is not described by a list of column names followed by type", group)
			return nil, err
		}
		elementNames := strings.Split(twoParts[0], ",")
		elementType := twoParts[1]
		eType := EnumElementTypeFromName(elementType)
		if eType == NONE {
			err = fmt.Errorf("error: %s: Data type is not a supported type", group)
			return nil, err
		}
		for _, name := range elementNames {
			dsa = append(dsa, DataShape{Name: name, Type: eType})
		}
	}
	return dsa, nil
}

func (ds *DataShape) toBytes() ([]byte, error) {
	buffer := make([]byte, 0)

	nameLen := uint8(len(ds.Name))

	// byte length of the data shape name
	buffer, err := Serialize(buffer, nameLen)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize length of data shape name:"+ds.Name)
	}

	// data shape name
	buffer, err = Serialize(buffer, ds.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize column name:"+ds.Name)
	}

	// data type
	buffer, err = Serialize(buffer, byte(ds.Type))
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize data type:"+string(ds.Type))
	}
	return buffer, nil
}

// dsFromBytes deserializes bytes into a DataShape and return it with its byte length.
func dsFromBytes(buf []byte) (ds DataShape, cursor int) {
	cursor = 0
	dsNameLen := int(ToUint8(buf[cursor : cursor+1]))
	cursor++
	dsName := ToString(buf[cursor : cursor+dsNameLen])
	cursor += dsNameLen
	dsType := EnumElementType(buf[cursor])
	cursor++

	return DataShape{Name: dsName, Type: dsType}, cursor
}

// DSVFromBytes deserializes bytes into an array of datashape (=Data Shape Vector)
// and return it with its byte length.
func DSVFromBytes(buf []byte) (dataShape []DataShape, byteLength int) {
	if buf == nil {
		return nil, 0
	}

	cursor := 0
	dsLen := int(ToUint8(buf[cursor : cursor+1]))
	ret := make([]DataShape, dsLen)

	// deserializes each data shape
	cursor++
	for i := 0; i < dsLen; i++ {
		ds, l := dsFromBytes(buf[cursor:])
		ret[i] = ds
		cursor += l
	}
	return ret, cursor
}

// DSVToBytes serializes an array of DataShape (=Data Shape Vector) into []byte.
func DSVToBytes(dss []DataShape) ([]byte, error) {
	dsLen := uint8(len(dss))
	if dsLen == 0 {
		return nil, nil
	}

	buffer := make([]byte, 0)
	// Length of the data shapes
	buffer, err := Serialize(buffer, dsLen)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize data shape length: "+string(dsLen))
	}

	// append each serialized data shape
	for _, ds := range dss {
		b, err := ds.toBytes()
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize data shape: "+ds.String())
		}
		buffer = append(buffer, b...)
	}

	return buffer, nil
}
