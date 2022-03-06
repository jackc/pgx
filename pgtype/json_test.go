package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func isExpectedEqMap(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		aa := a.(map[string]interface{})
		bb := v.(map[string]interface{})

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

	testutil.RunTranscodeTests(t, "json", []testutil.TranscodeTestCase{
		{[]byte("{}"), new([]byte), isExpectedEqBytes([]byte("{}"))},
		{[]byte("null"), new([]byte), isExpectedEqBytes([]byte("null"))},
		{[]byte("42"), new([]byte), isExpectedEqBytes([]byte("42"))},
		{[]byte(`"hello"`), new([]byte), isExpectedEqBytes([]byte(`"hello"`))},
		{[]byte(`"hello"`), new(string), isExpectedEq(`"hello"`)},
		{map[string]interface{}{"foo": "bar"}, new(map[string]interface{}), isExpectedEqMap(map[string]interface{}{"foo": "bar"})},
		{jsonStruct{Name: "Adam", Age: 10}, new(jsonStruct), isExpectedEq(jsonStruct{Name: "Adam", Age: 10})},
		{nil, new(*jsonStruct), isExpectedEq((*jsonStruct)(nil))},
		{map[string]interface{}(nil), new(*string), isExpectedEq((*string)(nil))},
		{map[string]interface{}(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})
}
