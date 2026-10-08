//go:build go1.27

package stdlib

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Rows implements driver.RowsColumnScanner as of Go 1.27.
var _ driver.RowsColumnScanner = (*Rows)(nil)

// NextRow implements the driver.RowsColumnScanner interface. It advances to the
// next row of data and returns io.EOF when there are no more rows.
func (r *Rows) NextRow() error {
	var more bool
	if r.skipNext {
		more = r.skipNextMore
		r.skipNext = false
	} else {
		more = r.rows.Next()
	}

	if !more {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	return nil
}

// ScanColumn implements the driver.RowsColumnScanner interface. It preserves
// database/sql conversions for scalar destinations and sql.Scanner implementations.
// Other destinations, such as Go slices, pgtype.Array, and pgtype.Range, are
// scanned directly using the pgx type map.
func (r *Rows) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	if dest == nil {
		return errors.New("destination not a pointer")
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return errors.New("destination pointer is nil")
	}

	src := r.rows.RawValues()[index]
	if isSQLScanDestination(dest) {
		var value driver.Value
		if src != nil {
			r.initValueFuncs()
			var err error
			value, err = r.valueFuncs[index](src)
			if err != nil {
				return err
			}
		}
		return sql.ConvertAssign(scanCtx, dest, value)
	}

	m := r.conn.conn.TypeMap()
	fd := r.rows.FieldDescriptions()[index]
	return m.Scan(fd.DataTypeOID, fd.Format, src, dest)
}

// isSQLScanDestination includes named scalar types and nullable pointers to
// database/sql destinations. Select the conversion before scanning: retrying a
// failed scan could call user code twice or discard its error.
func isSQLScanDestination(dest any) bool {
	if isDest, ok := classifyScanDestination(dest); ok {
		return isDest
	}

	t := reflect.TypeOf(dest)
	var namedPointers map[reflect.Type]bool
	for t.Kind() == reflect.Pointer {
		named := t.Name() != ""
		// Only defined pointer types can form a cycle without reaching a
		// non-pointer type. Leave those to pgx's bounded scan planning.
		if named {
			if namedPointers[t] {
				return false
			}
			if namedPointers == nil {
				namedPointers = make(map[reflect.Type]bool)
			}
			namedPointers[t] = true
		}
		t = t.Elem()

		switch {
		case t.Kind() == reflect.Pointer:
			// A nil pointer of type t has the full method set of t, and an
			// interface holds it without an allocation.
			if isDest, ok := classifyScanDestination(reflect.Zero(t).Interface()); ok {
				return isDest
			}
		// An unnamed pointer's method set contains its element's method set,
		// so only an interface element, or the element of a defined pointer
		// type, which has no methods, needs the checks. A zero value cannot
		// replace Implements here: a nil interface has no dynamic type.
		case t.Kind() == reflect.Interface || named:
			if t.Implements(reflect.TypeFor[sql.Scanner]()) {
				return true
			}
			if t.Implements(reflect.TypeFor[pgtype.ArraySetter]()) || t.Implements(reflect.TypeFor[pgtype.BytesScanner]()) {
				return false
			}
		}
	}

	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String, reflect.Interface:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.Uint8
	case reflect.Struct:
		return t.ConvertibleTo(reflect.TypeFor[time.Time]())
	default:
		return false
	}
}

// classifyScanDestination reports whether the interfaces that v implements
// select the database/sql conversion. ok is false if v implements none of
// them. ScanColumn calls this for every column of every row. A type switch
// uses the runtime's cache, but reflect.Type.Implements walks the method set
// on every call.
func classifyScanDestination(v any) (isDest, ok bool) {
	switch v.(type) {
	case sql.Scanner:
		return true, true
	// These interfaces distinguish native byte slices, such as
	// FlatArray[byte] and PreallocBytes, from ordinary []byte destinations.
	case pgtype.ArraySetter, pgtype.BytesScanner:
		return false, true
	}
	return false, false
}
