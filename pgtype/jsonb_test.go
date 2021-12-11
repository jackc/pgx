package pgtype_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestJSONBTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)
	if _, ok := conn.ConnInfo().DataTypeForName("jsonb"); !ok {
		t.Skip("Skipping due to no jsonb type")
	}

	testutil.TestSuccessfulTranscode(t, "jsonb", []interface{}{
		&pgtype.JSONB{Bytes: []byte("{}"), Valid: true},
		&pgtype.JSONB{Bytes: []byte("null"), Valid: true},
		&pgtype.JSONB{Bytes: []byte("42"), Valid: true},
		&pgtype.JSONB{Bytes: []byte(`"hello"`), Valid: true},
		&pgtype.JSONB{},
	})
}

func TestJSONBSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.JSONB
	}{
		{source: "{}", result: pgtype.JSONB{Bytes: []byte("{}"), Valid: true}},
		{source: []byte("{}"), result: pgtype.JSONB{Bytes: []byte("{}"), Valid: true}},
		{source: ([]byte)(nil), result: pgtype.JSONB{}},
		{source: (*string)(nil), result: pgtype.JSONB{}},
		{source: []int{1, 2, 3}, result: pgtype.JSONB{Bytes: []byte("[1,2,3]"), Valid: true}},
		{source: map[string]interface{}{"foo": "bar"}, result: pgtype.JSONB{Bytes: []byte(`{"foo":"bar"}`), Valid: true}},
	}

	for i, tt := range successfulTests {
		var d pgtype.JSONB
		err := d.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(d, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, d)
		}
	}
}

func TestJSONBAssignTo(t *testing.T) {
	var s string
	var ps *string
	var b []byte

	rawStringTests := []struct {
		src      pgtype.JSONB
		dst      *string
		expected string
	}{
		{src: pgtype.JSONB{Bytes: []byte("{}"), Valid: true}, dst: &s, expected: "{}"},
	}

	for i, tt := range rawStringTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if *tt.dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, *tt.dst)
		}
	}

	rawBytesTests := []struct {
		src      pgtype.JSONB
		dst      *[]byte
		expected []byte
	}{
		{src: pgtype.JSONB{Bytes: []byte("{}"), Valid: true}, dst: &b, expected: []byte("{}")},
		{src: pgtype.JSONB{}, dst: &b, expected: (([]byte)(nil))},
	}

	for i, tt := range rawBytesTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if bytes.Compare(tt.expected, *tt.dst) != 0 {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, *tt.dst)
		}
	}

	var mapDst map[string]interface{}
	type structDst struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	var strDst structDst

	unmarshalTests := []struct {
		src      pgtype.JSONB
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.JSONB{Bytes: []byte(`{"foo":"bar"}`), Valid: true}, dst: &mapDst, expected: map[string]interface{}{"foo": "bar"}},
		{src: pgtype.JSONB{Bytes: []byte(`{"name":"John","age":42}`), Valid: true}, dst: &strDst, expected: structDst{Name: "John", Age: 42}},
	}
	for i, tt := range unmarshalTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); !reflect.DeepEqual(dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	pointerAllocTests := []struct {
		src      pgtype.JSONB
		dst      **string
		expected *string
	}{
		{src: pgtype.JSONB{}, dst: &ps, expected: ((*string)(nil))},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if *tt.dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, *tt.dst)
		}
	}
}
