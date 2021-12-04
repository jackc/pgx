package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestJSONBArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "jsonb[]", []interface{}{
		&pgtype.JSONBArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.JSONBArray{},
		&pgtype.JSONBArray{
			Elements: []pgtype.JSONB{
				{Bytes: []byte(`"foo"`), Valid: true},
				{Bytes: []byte("null"), Valid: true},
				{Bytes: []byte("42"), Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
			Valid:      true,
		},
	})
}
