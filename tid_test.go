package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestTIDTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "tid", []interface{}{
		&pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Status: pgtype.Present},
		&pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Status: pgtype.Present},
		&pgtype.TID{Status: pgtype.Null},
	})
}

func TestTIDAssignTo(t *testing.T) {
	var s string
	var sp *string

	simpleTests := []struct {
		src      pgtype.TID
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Status: pgtype.Present}, dst: &s, expected: "(42,43)"},
		{src: pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Status: pgtype.Present}, dst: &s, expected: "(4294967295,65535)"},
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
		src      pgtype.TID
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Status: pgtype.Present}, dst: &sp, expected: "(42,43)"},
		{src: pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Status: pgtype.Present}, dst: &sp, expected: "(4294967295,65535)"},
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
}

