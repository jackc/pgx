package pgtype_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestTimestamptzTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "timestamptz", []interface{}{
		&pgtype.Timestamptz{Time: time.Date(1800, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1905, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1940, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1960, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		&pgtype.Timestamptz{Status: pgtype.Null},
		&pgtype.Timestamptz{Status: pgtype.Present, InfinityModifier: pgtype.Infinity},
		&pgtype.Timestamptz{Status: pgtype.Present, InfinityModifier: -pgtype.Infinity},
	}, func(a, b interface{}) bool {
		at := a.(pgtype.Timestamptz)
		bt := b.(pgtype.Timestamptz)

		return at.Time.Equal(bt.Time) && at.Status == bt.Status && at.InfinityModifier == bt.InfinityModifier
	})
}

func TestTimestamptzNanosecondsTruncated(t *testing.T) {
	tests := []struct {
		input    time.Time
		expected time.Time
	}{
		{time.Date(2020, 1, 1, 0, 0, 0, 999999999, time.Local), time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local)},
		{time.Date(2020, 1, 1, 0, 0, 0, 999999001, time.Local), time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local)},
	}
	for i, tt := range tests {
		{
			tstz := pgtype.Timestamptz{Time: tt.input, Status: pgtype.Present}
			buf, err := tstz.EncodeText(nil, nil)
			if err != nil {
				t.Errorf("%d. EncodeText failed - %v", i, err)
			}

			tstz.DecodeText(nil, buf)
			if err != nil {
				t.Errorf("%d. DecodeText failed - %v", i, err)
			}

			if !(tstz.Status == pgtype.Present && tstz.Time.Equal(tt.expected)) {
				t.Errorf("%d. EncodeText did not truncate nanoseconds", i)
			}
		}

		{
			tstz := pgtype.Timestamptz{Time: tt.input, Status: pgtype.Present}
			buf, err := tstz.EncodeBinary(nil, nil)
			if err != nil {
				t.Errorf("%d. EncodeBinary failed - %v", i, err)
			}

			tstz.DecodeBinary(nil, buf)
			if err != nil {
				t.Errorf("%d. DecodeBinary failed - %v", i, err)
			}

			if !(tstz.Status == pgtype.Present && tstz.Time.Equal(tt.expected)) {
				t.Errorf("%d. EncodeBinary did not truncate nanoseconds", i)
			}
		}
	}
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestamptzDecodeTextInvalid(t *testing.T) {
	tstz := &pgtype.Timestamptz{}
	err := tstz.DecodeText(nil, []byte(`eeeee`))
	require.Error(t, err)
}

func TestTimestamptzSet(t *testing.T) {
	type _time time.Time

	successfulTests := []struct {
		source interface{}
		result pgtype.Timestamptz
	}{
		{source: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(1999, 12, 31, 12, 59, 59, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1999, 12, 31, 12, 59, 59, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2000, 1, 1, 0, 0, 1, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 1, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: _time(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)), result: pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: pgtype.Infinity, result: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Status: pgtype.Present}},
		{source: pgtype.NegativeInfinity, result: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Timestamptz
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestTimestamptzAssignTo(t *testing.T) {
	var tim time.Time
	var ptim *time.Time

	simpleTests := []struct {
		src      pgtype.Timestamptz
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Timestamptz{Time: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}, dst: &tim, expected: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local)},
		{src: pgtype.Timestamptz{Time: time.Time{}, Status: pgtype.Null}, dst: &ptim, expected: ((*time.Time)(nil))},
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
		src      pgtype.Timestamptz
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Timestamptz{Time: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}, dst: &ptim, expected: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local)},
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
		src pgtype.Timestamptz
		dst interface{}
	}{
		{src: pgtype.Timestamptz{Time: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local), InfinityModifier: pgtype.Infinity, Status: pgtype.Present}, dst: &tim},
		{src: pgtype.Timestamptz{Time: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local), InfinityModifier: pgtype.NegativeInfinity, Status: pgtype.Present}, dst: &tim},
		{src: pgtype.Timestamptz{Time: time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Null}, dst: &tim},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}

func TestTimestamptzMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Timestamptz
		result string
	}{
		{source: pgtype.Timestamptz{Status: pgtype.Null}, result: "null"},
		{source: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Status: pgtype.Present}, result: "\"2012-03-29T10:05:45-06:00\""},
		{source: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Status: pgtype.Present}, result: "\"2012-03-29T10:05:45.555-06:00\""},
		{source: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Status: pgtype.Present}, result: "\"infinity\""},
		{source: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Status: pgtype.Present}, result: "\"-infinity\""},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestTimestamptzUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Timestamptz
	}{
		{source: "null", result: pgtype.Timestamptz{Status: pgtype.Null}},
		{source: "\"2012-03-29T10:05:45-06:00\"", result: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Status: pgtype.Present}},
		{source: "\"2012-03-29T10:05:45.555-06:00\"", result: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Status: pgtype.Present}},
		{source: "\"infinity\"", result: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Status: pgtype.Present}},
		{source: "\"-infinity\"", result: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Status: pgtype.Present}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Timestamptz
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !r.Time.Equal(tt.result.Time) || r.Status != tt.result.Status || r.InfinityModifier != tt.result.InfinityModifier {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
