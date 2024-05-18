package anynil

import (
	"database/sql/driver"
	"reflect"
)

// valuerReflectType is a reflect.Type for driver.Valuer. It has confusing syntax because reflect.TypeOf returns nil
// when it's argument is a nil interface value. So we use a pointer to the interface and call Elem to get the actual
// type. Yuck.
//
// This can be simplified in Go 1.22 with reflect.TypeFor.
//
// var valuerReflectType = reflect.TypeFor[driver.Valuer]()
var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// Is returns true if value is any type of nil except a pointer that directly implements driver.Valuer. e.g. nil,
// []byte(nil), and a *T where T implements driver.Valuer get normalized to nil but a *T where *T implements
// driver.Valuer does not.
func Is(value any) bool {
	if value == nil {
		return true
	}

	refVal := reflect.ValueOf(value)
	kind := refVal.Kind()
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		if !refVal.IsNil() {
			return false
		}

		if kind == reflect.Ptr {
			if _, ok := value.(driver.Valuer); ok {
				// The pointer will be considered to implement driver.Valuer even if it is actually implemented on the value.
				// But we only want to consider it nil if it is implemented on the pointer. So check if what the pointer points
				// to implements driver.Valuer.
				if !refVal.Type().Elem().Implements(valuerReflectType) {
					return false
				}
			}
		}

		return true
	default:
		return false
	}
}

// Normalize converts typed nils (e.g. []byte(nil)) into untyped nil. Other values are returned unmodified.
func Normalize(v any) any {
	if Is(v) {
		return nil
	}
	return v
}

// NormalizeSlice converts all typed nils (e.g. []byte(nil)) in s into untyped nils. Other values are unmodified. s is
// mutated in place.
func NormalizeSlice(s []any) {
	for i := range s {
		if Is(s[i]) {
			s[i] = nil
		}
	}
}
