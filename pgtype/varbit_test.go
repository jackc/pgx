package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestVarbitTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "varbit", []interface{}{
		pgtype.Varbit{Bytes: []byte{}, Len: 0, Status: pgtype.Present},
		pgtype.Varbit{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Status: pgtype.Present},
		pgtype.Varbit{Status: pgtype.Null},
	})
}

func TestVarbitNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql:   "select B'1010101010",
			value: pgtype.Varbit{Bytes: []byte{1, 2}, Len: 10, Status: pgtype.Present},
		},
	})
}
