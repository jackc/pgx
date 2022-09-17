package anynil

import "reflect"

// Is returns true if value is any type of nil. e.g. nil or []byte(nil).
func Is(value any) bool {
	if value == nil {
		return true
	}

	refVal := reflect.ValueOf(value)
	switch refVal.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return refVal.IsNil()
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
