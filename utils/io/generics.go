package io

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"unsafe"
)

type QuorumValue struct {
	contents  map[int]interface{}
	histogram map[int]int
}

func NewQuorumValue() *QuorumValue {
	qv := new(QuorumValue)
	qv.contents = make(map[int]interface{})
	qv.histogram = make(map[int]int)
	return qv
}

func (qv *QuorumValue) AddValue(ival interface{}) error {
	buf := []byte{}
	var err error
	val := reflect.ValueOf(ival)
	switch val.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			buf, err = Serialize(buf, val.Index(i).Interface())
			if err != nil {
				return err
			}
		}
	case reflect.Map, reflect.Struct, reflect.Chan, reflect.Interface, reflect.Ptr, reflect.UnsafePointer:
		return fmt.Errorf("Unable to handle type: %v", val.Kind())
	default:
		buf, err = Serialize(buf, val)
		if err != nil {
			return err
		}
	}
	hasher := fnv.New32()
	_, err = hasher.Write(buf)
	if err != nil {
		return err
	}
	tval := hasher.Sum(nil)
	hashval := int(*((*uint32)(unsafe.Pointer(&tval[0]))))

	/*
		Note that this assumes the vals stored here are stable, i.e. not changing due to GC, etc
	*/
	if _, ok := qv.histogram[hashval]; !ok {
		qv.histogram[hashval] = 1
		qv.contents[hashval] = ival
	} else {
		qv.histogram[hashval]++
	}
	//	fmt.Printf("Buf: %v Hash: %v Histo: %v\n", buf, hashval, qv.histogram)
	return nil
}

func (qv *QuorumValue) GetTopValue() (val interface{}, confidence int) {
	if len(qv.contents) == 0 {
		return nil, 0
	}
	var maxCount, topKey int
	for key, count := range qv.histogram {
		if count > maxCount {
			maxCount = count
			topKey = key
		}
	}
	return qv.contents[topKey], maxCount
}

type AnySet struct {
	orderedElems interface{}
	mappedElems  interface{}
}

func NewAnySet(i_elems interface{}) (as *AnySet, err error) {
	/*
		Generic Set
		Input should be a slice of any hashable type
	*/
	refValue := reflect.ValueOf(i_elems)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return nil, fmt.Errorf("Unable to build set from non-slice type")
	}

	if refValue.Len() == 0 {
		return nil, fmt.Errorf("Empty input to AnySet")
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

func (as *AnySet) Add(i_elem interface{}) {
	/*
		Single element as input - must be of same type as existing
	*/
	refValue := reflect.ValueOf(i_elem)
	v_map := reflect.ValueOf(as.mappedElems)
	v_slice := reflect.ValueOf(as.orderedElems)
	emptyValue := reflect.ValueOf(nil)
	if v_map.MapIndex(refValue) == emptyValue {
		v_map.SetMapIndex(refValue, reflect.ValueOf(true))
		v_slice = reflect.Append(v_slice, refValue)
	}
	as.orderedElems = v_slice.Interface()
}
func (as *AnySet) Del(i_elem interface{}) {
	/*
		Single element as input - must be of same type as existing
	*/
	refValue := reflect.ValueOf(i_elem)
	refType := refValue.Type()
	v_map := reflect.ValueOf(as.mappedElems)
	v_slice := reflect.ValueOf(as.orderedElems)

	/*
		Make a new slice to hold the ordered remaining values
	*/
	emptyValue := reflect.ValueOf(nil)
	if v_map.MapIndex(refValue) != emptyValue {
		v_map.SetMapIndex(refValue, emptyValue) // Delete the key value
		newSlice := reflect.MakeSlice(reflect.SliceOf(refType), 0, 0)
		for i := 0; i < v_slice.Len(); i++ {
			elem := v_slice.Index(i)
			// Note that DeepEqual does not work between reflection
			// values elem and refValue - this is a workaround
			if v_map.MapIndex(elem) != emptyValue {
				newSlice = reflect.Append(newSlice, elem)
			}
		}
		as.orderedElems = newSlice.Interface()
	}
}

func (as *AnySet) Intersect(input interface{}) (out interface{}) {
	/*
		Slice as input - must be of same type as existing
	*/
	if as == nil {
		return nil
	}
	refValue := reflect.ValueOf(input)
	refType := refValue.Type()
	v_map := reflect.ValueOf(as.mappedElems)
	newSlice := reflect.MakeSlice(refType, 0, 0)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return fmt.Errorf("Unable to do intersect of a non-slice type")
	}

	if refValue.Len() == 0 {
		return fmt.Errorf("Empty input to intersect")
	}

	emptyValue := reflect.ValueOf(nil)
	for i := 0; i < refValue.Len(); i++ {
		name := refValue.Index(i)
		if v_map.MapIndex(name) != emptyValue { // Name is found in A
			newSlice = reflect.Append(newSlice, name)
		}
	}
	return newSlice.Interface()
}

func (as *AnySet) Subtract(input interface{}) (out interface{}) {
	/*
		Slice as input - must be of same type as existing
	*/
	/*
		Provides a list of all elements in A and not in B
			Output => A \ B
			"The Relative Complement of B in A"
		where B is input and A is the set
	*/
	refValue := reflect.ValueOf(input)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return fmt.Errorf("Unable to do intersect of a non-slice type")
	}

	if refValue.Len() == 0 {
		return as.orderedElems
	}

	firstElement := refValue.Index(0)
	newMapType := reflect.MapOf(firstElement.Type(), reflect.TypeOf(true))

	i_intersection := as.Intersect(input)
	intersection := reflect.ValueOf(i_intersection)

	intMap := reflect.MakeMap(newMapType)
	for i := 0; i < intersection.Len(); i++ {
		elem := intersection.Index(i)
		intMap.SetMapIndex(elem, reflect.ValueOf(true))
	}

	emptyValue := reflect.ValueOf(nil)
	newSlice := reflect.MakeSlice(
		reflect.SliceOf(firstElement.Type()),
		0, 0)
	orderedElems := reflect.ValueOf(as.orderedElems)
	for i := 0; i < orderedElems.Len(); i++ { // All of A
		elem := orderedElems.Index(i)
		if intMap.MapIndex(elem) == emptyValue { // Name is not found in A U B
			newSlice = reflect.Append(newSlice, elem)
		}
	}
	return newSlice.Interface()
}

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
	i_intersection := as.Intersect(input)
	intersection := reflect.ValueOf(i_intersection)
	return intersection.Len() == refValue.Len()
}

func DownSizeSlice(i_slice interface{}, newLen int, direction DirectionEnum) (i_out interface{}, err error) {
	refValue := reflect.ValueOf(i_slice)
	refType := reflect.TypeOf(i_slice)

	if !(refValue.Kind() == reflect.Slice ||
		refValue.Kind() == reflect.Array) {
		return nil, fmt.Errorf("Unable to resize non-slice type")
	}

	var oldLen = refValue.Len()
	if oldLen <= newLen {
		return i_slice, nil
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
	case left == nil && right == nil:
		fallthrough
	case left == nil: // Left should hold a value
		fallthrough
	case right == nil: // Left will always compare true to nil
		return false, fmt.Errorf("nil comparison value")
	case op == EQ:
		return reflect.DeepEqual(left, right), nil
	case op == NEQ:
		return !reflect.DeepEqual(left, right), nil
	}

	var l_float, r_float float64
	var l_int, r_int int64
	l_float, err = GetValueAsFloat64(left)
	if err == nil {
		r_float, err = GetValueAsFloat64(right)
		if err != nil {
			return false, fmt.Errorf("Left and right values do not match")
		}
		switch op {
		case LT:
			return l_float < r_float, nil
		case LTE:
			return l_float <= r_float, nil
		case GT:
			return l_float > r_float, nil
		case GTE:
			return l_float >= r_float, nil
		}
	}

	l_int, err = GetValueAsInt64(left)
	if err == nil {
		r_int, err = GetValueAsInt64(right)
		if err != nil {
			return false, fmt.Errorf("Left and right values do not match")
		}
		switch op {
		case LT:
			return l_int < r_int, nil
		case LTE:
			return l_int <= r_int, nil
		case GT:
			return l_int > r_int, nil
		case GTE:
			return l_int >= r_int, nil
		}
	}

	return false, nil
}

func GetValueAsFloat64(i_value interface{}) (val float64, err error) {
	switch value := i_value.(type) {
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
		return 0, fmt.Errorf("Not a float")
	}
	return val, nil
}

func GetValueAsInt64(i_value interface{}) (val int64, err error) {
	switch value := i_value.(type) {
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
		return 0, fmt.Errorf("Not an int")
	}
	return val, nil
}

/*
Utility datatypes
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
