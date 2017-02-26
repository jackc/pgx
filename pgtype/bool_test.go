package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestBoolTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "bool", []interface{}{
		pgtype.Bool{Bool: false, Status: pgtype.Present},
		pgtype.Bool{Bool: true, Status: pgtype.Present},
		pgtype.Bool{Bool: false, Status: pgtype.Null},
	})
}

func TestBoolConvertFrom(t *testing.T) {
	type _int8 int8

	successfulTests := []struct {
		source interface{}
		result pgtype.Bool
	}{
		{source: true, result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: false, result: pgtype.Bool{Bool: false, Status: pgtype.Present}},
		{source: "true", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "false", result: pgtype.Bool{Bool: false, Status: pgtype.Present}},
		{source: "t", result: pgtype.Bool{Bool: true, Status: pgtype.Present}},
		{source: "f", result: pgtype.Bool{Bool: false, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Bool
		err := r.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
