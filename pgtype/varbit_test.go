package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestVarbitTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "varbit", []interface{}{
		&pgtype.Varbit{Bytes: []byte{}, Len: 0, Status: pgtype.Present},
		&pgtype.Varbit{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Status: pgtype.Present},
		&pgtype.Varbit{Bytes: []byte{0, 1, 128, 254, 128}, Len: 33, Status: pgtype.Present},
		&pgtype.Varbit{Status: pgtype.Null},
	})
}

func TestVarbitNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql:   "select B'111111111'",
			value: &pgtype.Varbit{Bytes: []byte{255, 128}, Len: 9, Status: pgtype.Present},
		},
	})
}
