package pgtype_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestTimestamptzCodec(t *testing.T) {
	testutil.RunTranscodeTests(t, "timestamptz", []testutil.TranscodeTestCase{
		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local))},
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local))},
		{time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local))},
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local))},
		{time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local))},
		{time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), new(time.Time), isExpectedEqTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local))},

		// Nanosecond truncation
		{time.Date(2020, 1, 1, 0, 0, 0, 999999999, time.Local), new(time.Time), isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local))},
		{time.Date(2020, 1, 1, 0, 0, 0, 999999001, time.Local), new(time.Time), isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local))},

		{pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}, new(pgtype.Timestamptz), isExpectedEq(pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true})},
		{pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, new(pgtype.Timestamptz), isExpectedEq(pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{pgtype.Timestamptz{}, new(pgtype.Timestamptz), isExpectedEq(pgtype.Timestamptz{})},
		{nil, new(*time.Time), isExpectedEq((*time.Time)(nil))},
	})
}

// https://github.com/jackc/pgx/v4/pgtype/pull/128
func TestTimestamptzTranscodeBigTimeBinary(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	in := &pgtype.Timestamptz{Time: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), Valid: true}
	var out pgtype.Timestamptz

	err := conn.QueryRow(context.Background(), "select $1::timestamptz", in).Scan(&out)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, in.Valid, out.Valid)
	require.Truef(t, in.Time.Equal(out.Time), "expected %v got %v", in.Time, out.Time)
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestamptzDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestamptzCodec{}
	var tstz pgtype.Timestamptz
	plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz, false)
	err := plan.Scan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte(`eeeee`), &tstz)
	require.Error(t, err)
}

func TestTimestamptzMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Timestamptz
		result string
	}{
		{source: pgtype.Timestamptz{}, result: "null"},
		{source: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Valid: true}, result: "\"2012-03-29T10:05:45-06:00\""},
		{source: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Valid: true}, result: "\"2012-03-29T10:05:45.555-06:00\""},
		{source: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}, result: "\"infinity\""},
		{source: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, result: "\"-infinity\""},
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
		{source: "null", result: pgtype.Timestamptz{}},
		{source: "\"2012-03-29T10:05:45-06:00\"", result: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Valid: true}},
		{source: "\"2012-03-29T10:05:45.555-06:00\"", result: pgtype.Timestamptz{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Valid: true}},
		{source: "\"infinity\"", result: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}},
		{source: "\"-infinity\"", result: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Timestamptz
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !r.Time.Equal(tt.result.Time) || r.Valid != tt.result.Valid || r.InfinityModifier != tt.result.InfinityModifier {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
