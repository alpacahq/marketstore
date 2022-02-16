package io

import (
	"fmt"
	. "reflect"
)

// Serialize serializes various primitive types into a byte representation,
// appends it to the specified buffer and return it.
// Useful for output to files.
func Serialize(buffer []byte, datum interface{}) ([]byte, error) {
	if buffer == nil {
		buffer = make([]byte, 0)
	}
	// skip reflection & recurision if it's a byte slice
	if b, ok := datum.([]byte); ok {
		return append(buffer, b...), nil
	}

	// use reflection
	value := ValueOf(datum)
	var err error
	switch value.Kind() {
	case Chan, Func, Interface, Ptr, UnsafePointer:
		return buffer, fmt.Errorf("Serialize: Type %s is not serializable", value.Kind().String())
	case String:
		return append(buffer, datum.(string)...), nil
	case Struct:
		for i := 0; i < value.NumField(); i++ {
			subDatum := value.Field(i).Interface()
			buffer, err = Serialize(buffer, subDatum)
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	case Slice, Array:
		for i := 0; i < value.Len(); i++ {
			buffer, err = Serialize(buffer, value.Index(i).Interface())
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	case Map:
		for _, key := range value.MapKeys() {
			// We serialize the key length, then the key string, then the value
			buffer, err = Serialize(buffer, int16(key.Len()))
			if err != nil {
				return nil, err
			}
			buffer, err = Serialize(buffer, key.Interface())
			if err != nil {
				return nil, err
			}
			buffer, err = Serialize(buffer, value.MapIndex(key).Interface())
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	default:
		return append(buffer, DataToByteSlice(datum)...), nil
	}
}
