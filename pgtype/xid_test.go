package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestXIDTranscode(t *testing.T) {
	pgTypeName := "xid"
	values := []interface{}{
		&pgtype.XID{Uint: 42, Valid: true},
		&pgtype.XID{},
	}
	eqFunc := func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	}

	testutil.TestPgxSuccessfulTranscodeEqFunc(t, pgTypeName, values, eqFunc)
	testutil.TestDatabaseSQLSuccessfulTranscodeEqFunc(t, "github.com/jackc/pgx/stdlib", pgTypeName, values, eqFunc)
}

func TestXIDSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.XID
	}{
		{source: uint32(1), result: pgtype.XID{Uint: 1, Valid: true}},
	}

	for i, tt := range successfulTests {
		var r pgtype.XID
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestXIDAssignTo(t *testing.T) {
	var ui32 uint32
	var pui32 *uint32

	simpleTests := []struct {
		src      pgtype.XID
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.XID{Uint: 42, Valid: true}, dst: &ui32, expected: uint32(42)},
		{src: pgtype.XID{}, dst: &pui32, expected: ((*uint32)(nil))},
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
		src      pgtype.XID
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.XID{Uint: 42, Valid: true}, dst: &pui32, expected: uint32(42)},
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
		src pgtype.XID
		dst interface{}
	}{
		{src: pgtype.XID{}, dst: &ui32},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
