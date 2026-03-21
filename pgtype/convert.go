package pgtype

import (
	"reflect"
)

func NullAssignTo(dst any) error {
	dstPtr := reflect.ValueOf(dst)

	// AssignTo dst must always be a pointer
	if dstPtr.Kind() != reflect.Ptr {
		return &nullAssignmentError{dst: dst}
	}

	dstVal := dstPtr.Elem()

	switch dstVal.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map:
		dstVal.Set(reflect.Zero(dstVal.Type()))
		return nil
	}

	return &nullAssignmentError{dst: dst}
}

var kindTypes map[reflect.Kind]reflect.Type

func toInterface(dst reflect.Value, t reflect.Type) (any, bool) {
	nextDst := dst.Convert(t)
	return nextDst.Interface(), dst.Type() != nextDst.Type()
}

// GetAssignToDstType attempts to convert dst to something AssignTo can assign
// to. If dst is a pointer to pointer it allocates a value and returns the
// dereferences pointer. If dst is a named type such as *Foo where Foo is type
// Foo int16, it converts dst to *int16.
//
// GetAssignToDstType returns the converted dst and a bool representing if any
// change was made.
func GetAssignToDstType(dst any) (any, bool) {
	dstPtr := reflect.ValueOf(dst)

	// AssignTo dst must always be a pointer
	if dstPtr.Kind() != reflect.Ptr {
		return nil, false
	}

	dstVal := dstPtr.Elem()

	// if dst is a pointer to pointer, allocate space try again with the dereferenced pointer
	if dstVal.Kind() == reflect.Ptr {
		dstVal.Set(reflect.New(dstVal.Type().Elem()))
		return dstVal.Interface(), true
	}

	// if dst is pointer to a base type that has been renamed
	if baseValType, ok := kindTypes[dstVal.Kind()]; ok {
		return toInterface(dstPtr, reflect.PtrTo(baseValType))
	}

	if dstVal.Kind() == reflect.Slice {
		if baseElemType, ok := kindTypes[dstVal.Type().Elem().Kind()]; ok {
			return toInterface(dstPtr, reflect.PtrTo(reflect.SliceOf(baseElemType)))
		}
	}

	if dstVal.Kind() == reflect.Array {
		if baseElemType, ok := kindTypes[dstVal.Type().Elem().Kind()]; ok {
			return toInterface(dstPtr, reflect.PtrTo(reflect.ArrayOf(dstVal.Len(), baseElemType)))
		}
	}

	if dstVal.Kind() == reflect.Struct {
		if dstVal.Type().NumField() == 1 && dstVal.Type().Field(0).Anonymous {
			dstPtr = dstVal.Field(0).Addr()
			nested := dstVal.Type().Field(0).Type
			if nested.Kind() == reflect.Array {
				if baseElemType, ok := kindTypes[nested.Elem().Kind()]; ok {
					return toInterface(dstPtr, reflect.PtrTo(reflect.ArrayOf(nested.Len(), baseElemType)))
				}
			}
			if _, ok := kindTypes[nested.Kind()]; ok && dstPtr.CanInterface() {
				return dstPtr.Interface(), true
			}
		}
	}

	return nil, false
}

func init() {
	kindTypes = map[reflect.Kind]reflect.Type{
		reflect.Bool:    reflect.TypeFor[bool](),
		reflect.Float32: reflect.TypeFor[float32](),
		reflect.Float64: reflect.TypeFor[float64](),
		reflect.Int:     reflect.TypeFor[int](),
		reflect.Int8:    reflect.TypeFor[int8](),
		reflect.Int16:   reflect.TypeFor[int16](),
		reflect.Int32:   reflect.TypeFor[int32](),
		reflect.Int64:   reflect.TypeFor[int64](),
		reflect.Uint:    reflect.TypeFor[uint](),
		reflect.Uint8:   reflect.TypeFor[uint8](),
		reflect.Uint16:  reflect.TypeFor[uint16](),
		reflect.Uint32:  reflect.TypeFor[uint32](),
		reflect.Uint64:  reflect.TypeFor[uint64](),
		reflect.String:  reflect.TypeFor[string](),
	}
}
