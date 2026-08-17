package pgtype_test

import (
	"context"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestTimestamptzCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamptz", []pgxtest.ValueRoundTripTest{
		{Param: time.Date(-100, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-100, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(-1, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(1, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.Local))},

		{Param: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local))},
		{Param: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local))},

		// Nanosecond truncation
		{Param: time.Date(2020, 1, 1, 0, 0, 0, 999999999, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local))},
		{Param: time.Date(2020, 1, 1, 0, 0, 0, 999999001, time.Local), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.Local))},

		{Param: pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}, Result: new(pgtype.Timestamptz), Test: isExpectedEq(pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true})},
		{Param: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, Result: new(pgtype.Timestamptz), Test: isExpectedEq(pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{Param: pgtype.Timestamptz{}, Result: new(pgtype.Timestamptz), Test: isExpectedEq(pgtype.Timestamptz{})},
		{Param: nil, Result: new(*time.Time), Test: isExpectedEq((*time.Time)(nil))},
	})
}

func TestTimestamptzCodecWithLocationUTC(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name:  "timestamptz",
			OID:   pgtype.TimestamptzOID,
			Codec: &pgtype.TimestamptzCodec{ScanLocation: time.UTC},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, nil, "timestamptz", []pgxtest.ValueRoundTripTest{
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
	})
}

func TestTimestamptzCodecWithLocationLocal(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name:  "timestamptz",
			OID:   pgtype.TimestamptzOID,
			Codec: &pgtype.TimestamptzCodec{ScanLocation: time.Local},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, nil, "timestamptz", []pgxtest.ValueRoundTripTest{
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Result: new(time.Time), Test: isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local))},
	})
}

// https://github.com/jackc/pgx/v4/pgtype/pull/128
func TestTimestamptzTranscodeBigTimeBinary(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		in := &pgtype.Timestamptz{Time: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), Valid: true}
		var out pgtype.Timestamptz

		err := conn.QueryRow(ctx, "select $1::timestamptz", in).Scan(&out)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, in.Valid, out.Valid)
		require.Truef(t, in.Time.Equal(out.Time), "expected %v got %v", in.Time, out.Time)
	})
}

func TestTimestamptzCodecDecodeTextBigTime(t *testing.T) {
	c := &pgtype.TimestamptzCodec{ScanLocation: time.UTC}

	for _, tt := range []struct {
		src  string
		want time.Time
	}{
		{src: `10000-01-02 03:04:05.123456+00`, want: time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{src: `00000000000010000-01-02 03:04:05.123456+00`, want: time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{src: `294276-12-31 23:59:59.999999+00`, want: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		{src: `294276-12-31 23:59:59.999999499+00`, want: time.Date(294276, 12, 31, 23, 59, 59, 999999499, time.UTC)},
		{src: `294276-12-31 23:59:59.999999+14`, want: time.Date(294276, 12, 31, 9, 59, 59, 999999000, time.UTC)},
		{src: `294277-01-01 00:00:00+14`, want: time.Date(294276, 12, 31, 10, 0, 0, 0, time.UTC)},
		{src: `294277-01-01 15:58:59.999999+15:59`, want: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		{src: `4713-02-29 00:00:00+00 BC`, want: time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC)},
		{src: `4714-11-24 00:00:00+00 BC`, want: time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{src: `4714-11-23 10:00:00-14 BC`, want: time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{src: `4714-11-23 09:59:59.999999500-14 BC`, want: time.Date(-4713, 11, 23, 23, 59, 59, 999999500, time.UTC)},
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)

		err := plan.Scan([]byte(tt.src), &tstz)
		require.NoError(t, err)
		require.True(t, tstz.Valid)
		require.Equal(t, tt.want, tstz.Time)
	}
}

func TestTimestamptzCodecDecodeTextBigTimePreservesOffset(t *testing.T) {
	c := &pgtype.TimestamptzCodec{}

	for _, tt := range []struct {
		src  string
		want time.Time
	}{
		{src: `10000-01-02 03:04:05.123456+09`, want: time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.FixedZone("", 9*60*60))},
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)

		err := plan.Scan([]byte(tt.src), &tstz)
		require.NoError(t, err)
		require.True(t, tstz.Valid)

		_, offset := tstz.Time.Zone()
		require.Equal(t, 9*60*60, offset)
		require.Equal(t, tt.want, tstz.Time)
	}
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestamptzDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestamptzCodec{}

	for _, src := range []string{
		`eeeee`,
		`0000-01-01 00:00:00+00`,
		`10000-02-30 00:00:00+00`,
		`10001-02-29 00:00:00+00`,
		`2024-01-01 00:00:00+16`,
		`2024-01-01 00:00:00-16`,
		`294277-01-01 00:00:00+00`,
		`294277-01-01 15:59:00+15:59`,
		`294276-12-31 23:59:59.999999500+00`,
		`4714-01-01 00:00:00+00 BC`,
		`4714-11-23 23:59:59.999999499+00 BC`,
		`4714-11-23 09:59:59.999999499-14 BC`,
		`4714-11-24 00:00:00+14 BC`,
		`4712-02-29 00:00:00+00 BC`,
		`10000-01-01 00:00:00+00 BC`,
		`9223372036854775808-01-01 00:00:00+00`,
		`10000-01-02 03:04:05.123456+16`,
		`10000-01-02 03:04:05.123456-16`,
		`294276-12-31 23:59:59.999999-14`,
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)
		err := plan.Scan([]byte(src), &tstz)
		require.Error(t, err)
	}
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
