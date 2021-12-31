package pgtype

import (
	"fmt"
	"reflect"
)

type int16Array []int16

func (a int16Array) Dimensions() []ArrayDimension {
	if a == nil {
		return nil
	}

	return []ArrayDimension{{Length: int32(len(a)), LowerBound: 1}}
}

func (a int16Array) Index(i int) interface{} {
	return a[i]
}

func (a *int16Array) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		a = nil
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = make(int16Array, elementCount)
	return nil
}

func (a int16Array) ScanIndex(i int) interface{} {
	return &a[i]
}

type uint16Array []uint16

func (a uint16Array) Dimensions() []ArrayDimension {
	if a == nil {
		return nil
	}

	return []ArrayDimension{{Length: int32(len(a)), LowerBound: 1}}
}

func (a uint16Array) Index(i int) interface{} {
	return a[i]
}

func (a *uint16Array) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		a = nil
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = make(uint16Array, elementCount)
	return nil
}

func (a uint16Array) ScanIndex(i int) interface{} {
	return &a[i]
}

type anySliceArray struct {
	slice reflect.Value
}

func (a anySliceArray) Dimensions() []ArrayDimension {
	if a.slice.IsNil() {
		return nil
	}

	return []ArrayDimension{{Length: int32(a.slice.Len()), LowerBound: 1}}
}

func (a anySliceArray) Index(i int) interface{} {
	return a.slice.Index(i).Interface()
}

func (a *anySliceArray) SetDimensions(dimensions []ArrayDimension) error {
	sliceType := a.slice.Type()

	if dimensions == nil {
		a.slice.Set(reflect.Zero(sliceType))
		return nil
	}

	elementCount := cardinality(dimensions)
	slice := reflect.MakeSlice(sliceType, elementCount, elementCount)
	a.slice.Set(slice)
	return nil
}

func (a anySliceArray) ScanIndex(i int) interface{} {
	return a.slice.Index(i).Addr().Interface()
}

func makeArrayGetter(a interface{}) (ArrayGetter, error) {
	switch a := a.(type) {
	case ArrayGetter:
		return a, nil

	case []int16:
		return (*int16Array)(&a), nil

	case []uint16:
		return (*uint16Array)(&a), nil

	}

	reflectValue := reflect.ValueOf(a)
	if reflectValue.Kind() == reflect.Slice {
		return &anySliceArray{slice: reflectValue}, nil
	}

	return nil, fmt.Errorf("cannot convert %T to ArrayGetter", a)
}

func makeArraySetter(a interface{}) (ArraySetter, error) {
	switch a := a.(type) {
	case ArraySetter:
		return a, nil

	case *[]int16:
		return (*int16Array)(a), nil

	case *[]uint16:
		return (*uint16Array)(a), nil

	}

	value := reflect.ValueOf(a)
	if value.Kind() == reflect.Ptr {
		elemValue := value.Elem()
		if elemValue.Kind() == reflect.Slice {
			return &anySliceArray{slice: elemValue}, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T to ArraySetter", a)
}
