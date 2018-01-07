package io

type DataShape struct {
	Name string
	Type EnumElementType
}

func NewDataShapeVector(names []string, etypes []EnumElementType) (dsv []DataShape) {
	for i, name := range names {
		dsv = append(dsv, DataShape{name, etypes[i]})
	}
	return dsv
}

func (ds *DataShape) Len() (out int) {
	return ds.Type.Size()
}

func (ds *DataShape) String() (st string) {
	return ds.Name + ":" + ds.Type.String()
}
