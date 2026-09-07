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
	if isSQLScanDestination(rv.Type()) {
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
func isSQLScanDestination(t reflect.Type) bool {
	var namedPointers map[reflect.Type]bool
	for {
		if t.Implements(reflect.TypeFor[sql.Scanner]()) {
			return true
		}
		// These interfaces distinguish native byte slices, such as
		// FlatArray[byte] and PreallocBytes, from ordinary []byte destinations.
		if t.Implements(reflect.TypeFor[pgtype.ArraySetter]()) || t.Implements(reflect.TypeFor[pgtype.BytesScanner]()) {
			return false
		}
		if t.Kind() != reflect.Pointer {
			break
		}
		// Only defined pointer types can form a cycle without reaching a
		// non-pointer type. Leave those to pgx's bounded scan planning.
		if t.Name() != "" {
			if namedPointers[t] {
				return false
			}
			if namedPointers == nil {
				namedPointers = make(map[reflect.Type]bool)
			}
			namedPointers[t] = true
		}
		t = t.Elem()
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
