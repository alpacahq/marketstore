package io

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
