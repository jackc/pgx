package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestParseUntypedTextArray(t *testing.T) {
	tests := []struct {
		source string
		result pgtype.UntypedTextArray
	}{
		{
			source: "{}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{},
				Dimensions: []pgtype.ArrayDimension{},
			},
		},
		{
			source: "{1}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"1"},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{a,b}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"a", "b"},
				Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			},
		},
		{
			source: `{"NULL"}`,
			result: pgtype.UntypedTextArray{
				Elements:   []string{"NULL"},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{"He said, \"Hello.\""}`,
			result: pgtype.UntypedTextArray{
				Elements:   []string{`He said, "Hello."`},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
	}

	for i, tt := range tests {
		r, err := pgtype.ParseUntypedTextArray(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.source, tt.result, *r)
		}
	}
}
