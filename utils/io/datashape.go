package io

import (
	"fmt"
	"strings"
)

type DataShape struct {
	Name string
	Type EnumElementType
}

// NewDataShapeVector returns a new array of DataShapes for the given array of
// names and element types
func NewDataShapeVector(names []string, etypes []EnumElementType) (dsv []DataShape) {
	for i, name := range names {
		dsv = append(dsv, DataShape{name, etypes[i]})
	}
	return dsv
}

// Len returns the length of the DataShape
func (ds *DataShape) Len() (out int) {
	return ds.Type.Size()
}

// String returns the colon-separated string of the DataShapes name and type
func (ds *DataShape) String() (st string) {
	return ds.Name + ":" + ds.Type.String()
}

// Equal compares both the name and type of two DataShapes, only returning true
// if both are equal
func (ds *DataShape) Equal(shape DataShape) bool {
	return ds.Name == shape.Name && ds.Type == shape.Type
}

func DataShapesFromInputString(inputStr string) (dsa []DataShape, err error) {
	splitString := strings.Split(inputStr, ":")
	dsa = make([]DataShape, 0)
	for _, group := range splitString {
		twoParts := strings.Split(group, "/")
		if len(twoParts) != 2 {
			err = fmt.Errorf("error: %s: Data shape is not described by a list of column names followed by type.", group)
			fmt.Println(err.Error())
			return nil, err
		}
		elementNames := strings.Split(twoParts[0], ",")
		elementType := twoParts[1]
		eType := EnumElementTypeFromName(elementType)
		if eType == NONE {
			err = fmt.Errorf("error: %s: Data type is not a supported type", group)
			fmt.Println(err.Error())
			return nil, err
		}
		for _, name := range elementNames {
			dsa = append(dsa, DataShape{Name: name, Type: eType})
		}
	}
	return dsa, nil
}
