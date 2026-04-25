//go:build go1.27

package stdlib

import (
	"database/sql"
	"io"
)

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

// ScanColumn implements the driver.RowsColumnScanner interface. It uses the
// pgx type map to scan the raw bytes of the column at the given index directly
// into dest. This allows database/sql callers to scan into any type supported
// by pgx, such as Go slices, pgtype.Array, and pgtype.Range.
//
// When pgx does not have a scan plan for dest, ScanColumn falls back to
// sql.ConvertAssign on a driver.Value produced by the column codec. This gives
// database/sql callers the same conversion semantics they had before Go 1.27
// (e.g., scanning a PostgreSQL boolean into a *string).
func (r *Rows) ScanColumn(index int, dest any) error {
	m := r.conn.conn.TypeMap()
	fd := r.rows.FieldDescriptions()[index]
	src := r.rows.RawValues()[index]

	err := m.Scan(fd.DataTypeOID, fd.Format, src, dest)
	if err == nil {
		return nil
	}

	dt, ok := m.TypeForOID(fd.DataTypeOID)
	if !ok {
		return err
	}
	value, decodeErr := dt.Codec.DecodeDatabaseSQLValue(m, fd.DataTypeOID, fd.Format, src)
	if decodeErr != nil {
		return err
	}
	if convertErr := sql.ConvertAssign(dest, value); convertErr != nil {
		return err
	}
	return nil
}
