package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqMap(a any) func(any) bool {
	return func(v any) bool {
		aa := a.(map[string]any)
		bb := v.(map[string]any)

		if (aa == nil) != (bb == nil) {
			return false
		}

		if aa == nil {
			return true
		}

		if len(aa) != len(bb) {
			return false
		}

		for k := range aa {
			if aa[k] != bb[k] {
				return false
			}
		}

		return true
	}
}

func TestJSONCodec(t *testing.T) {
	type jsonStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "json", []pgxtest.ValueRoundTripTest{
		{nil, new(*jsonStruct), isExpectedEq((*jsonStruct)(nil))},
		{map[string]any(nil), new(*string), isExpectedEq((*string)(nil))},
		{map[string]any(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "json", []pgxtest.ValueRoundTripTest{
		{[]byte("{}"), new([]byte), isExpectedEqBytes([]byte("{}"))},
		{[]byte("null"), new([]byte), isExpectedEqBytes([]byte("null"))},
		{[]byte("42"), new([]byte), isExpectedEqBytes([]byte("42"))},
		{[]byte(`"hello"`), new([]byte), isExpectedEqBytes([]byte(`"hello"`))},
		{[]byte(`"hello"`), new(string), isExpectedEq(`"hello"`)},
		{map[string]any{"foo": "bar"}, new(map[string]any), isExpectedEqMap(map[string]any{"foo": "bar"})},
		{jsonStruct{Name: "Adam", Age: 10}, new(jsonStruct), isExpectedEq(jsonStruct{Name: "Adam", Age: 10})},
	})
}
