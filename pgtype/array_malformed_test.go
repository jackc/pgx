package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// A text array whose dimensions do not agree with the number of parsed
// elements (e.g. `{{{}0}}`) must return an error. The element scan loop
// indexes a value sized from the dimensions, so a mismatch previously
// panicked with a reflect index out of range.
func TestArrayTextDecodeDimensionElementMismatch(t *testing.T) {
	m := pgtype.NewMap()

	for _, src := range []string{`{{{}0}}`, `{{},{1}}`} {
		var v any
		plan := m.PlanScan(pgtype.Int4ArrayOID, pgtype.TextFormatCode, &v)
		require.NotNil(t, plan)
		require.Errorf(t, plan.Scan([]byte(src), &v), "src %q", src)
	}
}
