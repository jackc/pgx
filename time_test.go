package pgtype_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestTimeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "time", []interface{}{
		&pgtype.Time{Microseconds: 0, Status: pgtype.Present},
		&pgtype.Time{Microseconds: 1, Status: pgtype.Present},
		&pgtype.Time{Microseconds: 86399999999, Status: pgtype.Present},
		&pgtype.Time{Status: pgtype.Null},
	})
}

// Test for transcoding 24:00:00 separately as github.com/lib/pq doesn't seem to support it.
func TestTimeTranscode24HH(t *testing.T) {
	pgTypeName := "time"
	values := []interface{}{
		&pgtype.Time{Microseconds: 86400000000, Status: pgtype.Present},
	}

	eqFunc := func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	}

	testutil.TestPgxSuccessfulTranscodeEqFunc(t, pgTypeName, values, eqFunc)
	testutil.TestDatabaseSQLSuccessfulTranscodeEqFunc(t, "github.com/jackc/pgx/stdlib", pgTypeName, values, eqFunc)
}

func TestTimeSet(t *testing.T) {
	type _time time.Time

	successfulTests := []struct {
		source interface{}
		result pgtype.Time
	}{
		{source: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), result: pgtype.Time{Microseconds: 0, Status: pgtype.Present}},
		{source: time.Date(1900, 1, 1, 1, 0, 0, 0, time.UTC), result: pgtype.Time{Microseconds: 3600000000, Status: pgtype.Present}},
		{source: time.Date(1900, 1, 1, 0, 1, 0, 0, time.UTC), result: pgtype.Time{Microseconds: 60000000, Status: pgtype.Present}},
		{source: time.Date(1900, 1, 1, 0, 0, 1, 0, time.UTC), result: pgtype.Time{Microseconds: 1000000, Status: pgtype.Present}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC), result: pgtype.Time{Microseconds: 0, Status: pgtype.Present}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 1000, time.UTC), result: pgtype.Time{Microseconds: 1, Status: pgtype.Present}},
		{source: time.Date(1999, 12, 31, 23, 59, 59, 999999999, time.UTC), result: pgtype.Time{Microseconds: 86399999999, Status: pgtype.Present}},
		{source: time.Date(2015, 1, 1, 0, 0, 0, 2000, time.Local), result: pgtype.Time{Microseconds: 2, Status: pgtype.Present}},
		{source: func(t time.Time) *time.Time { return &t }(time.Date(2015, 1, 1, 0, 0, 0, 2000, time.Local)), result: pgtype.Time{Microseconds: 2, Status: pgtype.Present}},
		{source: nil, result: pgtype.Time{Status: pgtype.Null}},
		{source: (*time.Time)(nil), result: pgtype.Time{Status: pgtype.Null}},
		{source: _time(time.Date(1970, 1, 1, 0, 0, 0, 3000, time.UTC)), result: pgtype.Time{Microseconds: 3, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Time
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestTimeAssignTo(t *testing.T) {
	var tim time.Time
	var ptim *time.Time

	simpleTests := []struct {
		src      pgtype.Time
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Time{Microseconds: 0, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{src: pgtype.Time{Microseconds: 3600000000, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)},
		{src: pgtype.Time{Microseconds: 60000000, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC)},
		{src: pgtype.Time{Microseconds: 1000000, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC)},
		{src: pgtype.Time{Microseconds: 1, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 0, 0, 0, 1000, time.UTC)},
		{src: pgtype.Time{Microseconds: 86399999999, Status: pgtype.Present}, dst: &tim, expected: time.Date(2000, 1, 1, 23, 59, 59, 999999000, time.UTC)},
		{src: pgtype.Time{Microseconds: 0, Status: pgtype.Null}, dst: &ptim, expected: ((*time.Time)(nil))},
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
		src      pgtype.Time
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Time{Microseconds: 0, Status: pgtype.Present}, dst: &ptim, expected: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
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
		src pgtype.Time
		dst interface{}
	}{
		{src: pgtype.Time{Microseconds: 86400000000, Status: pgtype.Present}, dst: &tim},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
