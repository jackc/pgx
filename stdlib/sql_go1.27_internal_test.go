//go:build go1.27

package stdlib

import (
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

type cyclicPointer *cyclicPointer

type valueScanner struct{}

func (valueScanner) Scan(any) error { return nil }

type valueScannerPointer *valueScanner

type embedsNullStringPointer struct{ *sql.NullString }

func TestIsSQLScanDestination(t *testing.T) {
	t.Parallel()

	type namedString string

	var (
		s          string
		ps         *string
		ns         namedString
		b          []byte
		tm         time.Time
		ptm        *time.Time
		a          any
		ints       []int32
		nullStr    sql.NullString
		pNullStr   *sql.NullString
		flat       pgtype.FlatArray[string]
		pFlat      *pgtype.FlatArray[string]
		prealloc   pgtype.PreallocBytes
		scanner    sql.Scanner
		arraySet   pgtype.ArraySetter
		bytesScan  pgtype.BytesScanner
		cyclicDest cyclicPointer
		embedsNS   embedsNullStringPointer
		vs         valueScanner
		vsp        = valueScannerPointer(&vs)
	)

	for _, tt := range []struct {
		name string
		dest any
		want bool
	}{
		{"*string", &s, true},
		{"**string", &ps, true},
		{"*namedString", &ns, true},
		{"*[]byte", &b, true},
		{"*time.Time", &tm, true},
		{"**time.Time", &ptm, true},
		{"*any", &a, true},
		{"*[]int32", &ints, false},
		{"*sql.NullString", &nullStr, true},
		// The struct gets Scan from the embedded pointer.
		{"*embedsNullStringPointer", &embedsNS, true},
		// The element implements sql.Scanner, but the pointer to it does not.
		{"**sql.NullString", &pNullStr, true},
		{"*pgtype.FlatArray[string]", &flat, false},
		// The element implements pgtype.ArraySetter, but the pointer to it does not.
		{"**pgtype.FlatArray[string]", &pFlat, false},
		{"*pgtype.PreallocBytes", &prealloc, false},
		// An interface element can implement an interface that the pointer to it
		// does not.
		{"*sql.Scanner", &scanner, true},
		{"*pgtype.ArraySetter", &arraySet, false},
		{"*pgtype.BytesScanner", &bytesScan, false},
		{"*cyclicPointer", &cyclicDest, false},
		// A defined pointer type has no methods, so the checks continue on its
		// element, which implements sql.Scanner.
		{"valueScannerPointer", vsp, true},
		{"*valueScannerPointer", &vsp, true},
	} {
		assert.Equal(t, tt.want, isSQLScanDestination(tt.dest), tt.name)
	}
}
