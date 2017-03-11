package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestAclitemTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "aclitem", []interface{}{
		pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present},
		pgtype.Aclitem{String: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Status: pgtype.Present},
		pgtype.Aclitem{Status: pgtype.Null},
	})
}

func TestAclitemConvertFrom(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Aclitem
	}{
		{source: pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present}, result: pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present}},
		{source: "postgres=arwdDxt/postgres", result: pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present}},
		{source: (*string)(nil), result: pgtype.Aclitem{Status: pgtype.Null}},
	}

	for i, tt := range successfulTests {
		var d pgtype.Aclitem
		err := d.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if d != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, d)
		}
	}
}

func TestAclitemAssignTo(t *testing.T) {
	var s string
	var ps *string

	simpleTests := []struct {
		src      pgtype.Aclitem
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present}, dst: &s, expected: "postgres=arwdDxt/postgres"},
		{src: pgtype.Aclitem{Status: pgtype.Null}, dst: &ps, expected: ((*string)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	pointerAllocTests := []struct {
		src      pgtype.Aclitem
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Aclitem{String: "postgres=arwdDxt/postgres", Status: pgtype.Present}, dst: &ps, expected: "postgres=arwdDxt/postgres"},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src pgtype.Aclitem
		dst interface{}
	}{
		{src: pgtype.Aclitem{Status: pgtype.Null}, dst: &s},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
