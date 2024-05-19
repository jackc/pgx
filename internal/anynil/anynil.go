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

// Is returns true if value is any type of nil unless it implements driver.Valuer. *T is not considered to implement
// driver.Valuer if it is only implemented by T.
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

		if _, ok := value.(driver.Valuer); ok {
			if kind == reflect.Ptr {
				// The type assertion will succeed if driver.Valuer is implemented on T or *T. Check if it is implemented on T
				// to see if it is not implemented on *T.
				return refVal.Type().Elem().Implements(valuerReflectType)
			} else {
				return false
			}
		}

		return true
	default:
		return false
	}
}
