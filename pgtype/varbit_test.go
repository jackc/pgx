package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestVarbitTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "varbit", []interface{}{
		&pgtype.Varbit{Bytes: []byte{}, Len: 0, Valid: true},
		&pgtype.Varbit{Bytes: []byte{0, 1, 128, 254, 255}, Len: 40, Valid: true},
		&pgtype.Varbit{Bytes: []byte{0, 1, 128, 254, 128}, Len: 33, Valid: true},
		&pgtype.Varbit{},
	})
}

func TestVarbitNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL:   "select B'111111111'",
			Value: &pgtype.Varbit{Bytes: []byte{255, 128}, Len: 9, Valid: true},
		},
	})
}
