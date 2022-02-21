package io

import (
	"fmt"
	"reflect"
)

type AnySet struct {
	orderedElems interface{}
	mappedElems  interface{}
}

func NewAnySet(iElems interface{}) (as *AnySet, err error) {
	/*
		Generic Set
		Input should be a slice of any hashable type
	*/
	refValue := reflect.ValueOf(iElems)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return nil, fmt.Errorf("unable to build set from non-slice type")
	}

	if refValue.Len() == 0 {
		return nil, fmt.Errorf("empty input to AnySet")
	}

	firstElement := refValue.Index(0)
	newMapType := reflect.MapOf(firstElement.Type(), reflect.TypeOf(true))
	newMap := reflect.MakeMap(newMapType)
	newSlice := reflect.MakeSlice(
		reflect.SliceOf(firstElement.Type()),
		0, 0)
	for i := 0; i < refValue.Len(); i++ {
		newMap.SetMapIndex(refValue.Index(i), reflect.ValueOf(true))
		newSlice = reflect.Append(newSlice, refValue.Index(i))
	}

	as = new(AnySet)
	as.mappedElems = newMap.Interface()
	as.orderedElems = newSlice.Interface()
	return as, nil
}

func (as *AnySet) Add(iElem interface{}) {
	/*
		Single element as input - must be of same type as existing
	*/
	refValue := reflect.ValueOf(iElem)
	vMap := reflect.ValueOf(as.mappedElems)
	vSlice := reflect.ValueOf(as.orderedElems)
	if !vMap.MapIndex(refValue).IsValid() {
		vMap.SetMapIndex(refValue, reflect.ValueOf(true))
		vSlice = reflect.Append(vSlice, refValue)
	}
	as.orderedElems = vSlice.Interface()
}

func (as *AnySet) Del(iElem interface{}) {
	/*
		Single element as input - must be of same type as existing
	*/
	refValue := reflect.ValueOf(iElem)
	refType := refValue.Type()
	vMap := reflect.ValueOf(as.mappedElems)
	vSlice := reflect.ValueOf(as.orderedElems)

	/*
		Make a new slice to hold the ordered remaining values
	*/
	emptyValue := reflect.ValueOf(nil)
	if vMap.MapIndex(refValue).IsValid() {
		vMap.SetMapIndex(refValue, emptyValue) // Delete the key value
		newSlice := reflect.MakeSlice(reflect.SliceOf(refType), 0, 0)
		for i := 0; i < vSlice.Len(); i++ {
			elem := vSlice.Index(i)
			// Note that DeepEqual does not work between reflection
			// values elem and refValue - this is a workaround
			if vMap.MapIndex(elem).IsValid() {
				newSlice = reflect.Append(newSlice, elem)
			}
		}
		as.orderedElems = newSlice.Interface()
	}
}

// Intersect provides a list of all elements in both this object and input.
func (as *AnySet) Intersect(input interface{}) (out interface{}) {
	/*
		Slice as input - must be of same type as existing
	*/
	if as == nil {
		return nil
	}
	refValue := reflect.ValueOf(input)
	refType := refValue.Type()
	vMap := reflect.ValueOf(as.mappedElems)
	newSlice := reflect.MakeSlice(refType, 0, 0)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return fmt.Errorf("unable to do intersect of a non-slice type")
	}

	if refValue.Len() == 0 {
		return fmt.Errorf("empty input to intersect")
	}

	for i := 0; i < refValue.Len(); i++ {
		name := refValue.Index(i)
		if vMap.MapIndex(name).IsValid() { // Name is found in A
			newSlice = reflect.Append(newSlice, name)
		}
	}
	return newSlice.Interface()
}

// Subtract provides a list of all elements in this object and not in input.
func (as *AnySet) Subtract(inputSlice interface{}) (out interface{}) {
	/*
		Slice as input - must be of same type as existing
	*/
	/*
		Provides a list of all elements in A and not in B
			Output => A \ B
			"The Relative Complement of B in A"
		where B is input and A is the set
	*/
	refValue := reflect.ValueOf(inputSlice)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return fmt.Errorf("unable to do intersect of a non-slice type")
	}

	if refValue.Len() == 0 {
		return as.orderedElems
	}

	firstElement := refValue.Index(0)
	newMapType := reflect.MapOf(firstElement.Type(), reflect.TypeOf(true))

	iIntersection := as.Intersect(inputSlice)
	intersection := reflect.ValueOf(iIntersection)

	intMap := reflect.MakeMap(newMapType)
	for i := 0; i < intersection.Len(); i++ {
		elem := intersection.Index(i)
		intMap.SetMapIndex(elem, reflect.ValueOf(true))
	}

	newSlice := reflect.MakeSlice(
		reflect.SliceOf(firstElement.Type()),
		0, 0)
	orderedElems := reflect.ValueOf(as.orderedElems)
	for i := 0; i < orderedElems.Len(); i++ { // All of A
		elem := orderedElems.Index(i)
		intMap.MapIndex(elem)
		if !intMap.MapIndex(elem).IsValid() { // Name is not found in A ∩ B
			newSlice = reflect.Append(newSlice, elem)
		}
	}
	return newSlice.Interface()
}

// Contains returns True if the set fully contains the input.
func (as *AnySet) Contains(input interface{}) bool {
	/*
		Slice as input - must be of same type as existing
	*/
	refValue := reflect.ValueOf(input)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return false
	}

	if refValue.Len() == 0 {
		return false
	}

	/*
		True if set fully contains the input
	*/
	iIntersection := as.Intersect(input)
	intersection := reflect.ValueOf(iIntersection)
	return intersection.Len() == refValue.Len()
}

func DownSizeSlice(iSlice interface{}, newLen int, direction DirectionEnum) (iOut interface{}, err error) {
	refValue := reflect.ValueOf(iSlice)
	refType := reflect.TypeOf(iSlice)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return nil, fmt.Errorf("unable to resize non-slice type")
	}

	oldLen := refValue.Len()
	if oldLen <= newLen {
		return iSlice, nil
	}

	out := reflect.MakeSlice(refType, 0, 0)

	ibase := 0
	if direction == LAST {
		ibase = oldLen - newLen
	}

	for i := 0; i < newLen; i++ {
		ii := i + ibase
		out = reflect.Append(out, refValue.Index(ii))
	}

	return out.Interface(), nil
}

func GenericComparison(left, right interface{},
	op ComparisonOperatorEnum) (result bool, err error) {
	/*
		Evaluate: (left op right) as a boolean
	*/
	// Shortcut returns
	switch {
	case left == nil, right == nil: // should hold a value / will always compare true to nil
		return false, fmt.Errorf("nil comparison value")
	case op == EQ:
		return reflect.DeepEqual(left, right), nil
	case op == NEQ:
		return !reflect.DeepEqual(left, right), nil
	}

	var lFloat, rFloat float64
	var lInt, rInt int64
	lFloat, err = GetValueAsFloat64(left)
	if err == nil {
		rFloat, err = GetValueAsFloat64(right)
		if err != nil {
			return false, fmt.Errorf("left and right values do not match")
		}
		switch op {
		case LT:
			return lFloat < rFloat, nil
		case LTE:
			return lFloat <= rFloat, nil
		case GT:
			return lFloat > rFloat, nil
		case GTE:
			return lFloat >= rFloat, nil
		default:
		}
	}

	lInt, err = GetValueAsInt64(left)
	if err == nil {
		rInt, err = GetValueAsInt64(right)
		if err != nil {
			return false, fmt.Errorf("left and right values do not match")
		}
		switch op {
		case LT:
			return lInt < rInt, nil
		case LTE:
			return lInt <= rInt, nil
		case GT:
			return lInt > rInt, nil
		case GTE:
			return lInt >= rInt, nil
		default:
		}
	}

	return false, nil
}

func GetValueAsFloat64(iValue interface{}) (val float64, err error) {
	switch value := iValue.(type) {
	case int:
		val = float64(value)
	case int32:
		val = float64(value)
	case int64:
		val = float64(value)
	case float32:
		val = float64(value)
	case float64:
		val = value
	default:
		return 0, fmt.Errorf("not a float")
	}
	return val, nil
}

func GetValueAsInt64(iValue interface{}) (val int64, err error) {
	switch value := iValue.(type) {
	case int:
		val = int64(value)
	case int32:
		val = int64(value)
	case int64:
		val = value
	case float32:
		val = int64(value)
	case float64:
		val = int64(value)
	default:
		return 0, fmt.Errorf("not an int")
	}
	return val, nil
}

/*
Utility datatypes.
*/

type ComparisonOperatorEnum uint8

const (
	_ ComparisonOperatorEnum = iota
	EQ
	NEQ
	LT
	LTE
	GT
	GTE
)

func StringToComparisonOperatorEnum(opstr string) (oper ComparisonOperatorEnum) {
	switch opstr {
	case "=":
		return EQ
	case "<>", "!=":
		return NEQ
	case "<":
		return LT
	case "<=":
		return LTE
	case ">":
		return GT
	case ">=":
		return GTE
	default:
		return 0
	}
}

func (co ComparisonOperatorEnum) String() string {
	switch co {
	case EQ:
		return "="
	case NEQ:
		return "!="
	case LT:
		return "<"
	case LTE:
		return "<="
	case GT:
		return ">"
	case GTE:
		return ">="
	default:
		return "NONE"
	}
}
