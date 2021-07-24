package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestJSONBArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "jsonb[]", []interface{}{
		&pgtype.JSONBArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.JSONBArray{Status: pgtype.Null},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Status: pgtype.Present},
				{Bytes: []byte("null"), Status: pgtype.Present},
				{Bytes: []byte("42"), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
			Status:     pgtype.Present,
		},
	})
}
