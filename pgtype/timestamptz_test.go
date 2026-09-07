package pgtype_test

import (
	"context"
	"encoding/binary"
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

func TestTimestamptzCodecDecodeText(t *testing.T) {
	c := &pgtype.TimestamptzCodec{ScanLocation: time.UTC}

	for _, tt := range []struct {
		src  string
		want time.Time
	}{
		{`2024-01-02 03:04:05.123456+00`, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{`2024-01-02 03:04:05-05`, time.Date(2024, 1, 2, 8, 4, 5, 0, time.UTC)},
		{`2024-01-02 03:04:05+05:30`, time.Date(2024, 1, 1, 21, 34, 5, 0, time.UTC)},

		// Offsets with seconds show up for timestamps that predate standard time zones.
		{`1883-11-18 12:03:57-04:56:02`, time.Date(1883, 11, 18, 16, 59, 59, 0, time.UTC)},

		{`10000-01-02 03:04:05.123456+00`, time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{`294276-12-31 23:59:59.999999+00`, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},

		// The offset is applied before the range is checked, so a value whose displayed
		// year is past the maximum can still be inside it.
		{`294277-01-01 00:00:00+14`, time.Date(294276, 12, 31, 10, 0, 0, 0, time.UTC)},
		{`294277-01-01 15:58:59.999999+15:59`, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},

		{`0001-01-01 00:00:00+00 BC`, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{`4713-02-29 00:00:00+00 BC`, time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC)},
		{`4714-11-24 00:00:00+00 BC`, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{`4714-11-23 10:00:00-14 BC`, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)

		err := plan.Scan([]byte(tt.src), &tstz)
		require.NoErrorf(t, err, "%s", tt.src)
		require.Truef(t, tstz.Valid, "%s", tt.src)
		require.Equalf(t, tt.want, tstz.Time, "%s", tt.src)
	}
}

// TestTimestamptzCodecDecodeTextLocation covers the time zone rule for the text path: the
// parsed offset fixes the instant but is not kept as the location, because the binary path
// returns time.Local and the two formats have to agree.
func TestTimestamptzCodecDecodeTextLocation(t *testing.T) {
	scan := func(c *pgtype.TimestamptzCodec, src string) time.Time {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)
		require.NoError(t, plan.Scan([]byte(src), &tstz))
		return tstz.Time
	}

	got := scan(&pgtype.TimestamptzCodec{}, `2024-01-02 03:04:05+05:30`)
	require.Equal(t, time.Local, got.Location())
	require.True(t, time.Date(2024, 1, 1, 21, 34, 5, 0, time.UTC).Equal(got))

	got = scan(&pgtype.TimestamptzCodec{ScanLocation: time.UTC}, `2024-01-02 03:04:05+05:30`)
	require.Equal(t, time.UTC, got.Location())
	require.True(t, time.Date(2024, 1, 1, 21, 34, 5, 0, time.UTC).Equal(got))
}

func TestTimestamptzCodecEncodeText(t *testing.T) {
	m := pgtype.NewMap()

	for _, tt := range []struct {
		src  time.Time
		want string
	}{
		{time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC), `2024-01-02 03:04:05.123456Z`},
		{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), `2024-01-02 03:04:05Z`},
		{time.Date(2024, 1, 2, 3, 4, 5, 500000000, time.UTC), `2024-01-02 03:04:05.5Z`},
		{time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("", 5*60*60+30*60)), `2024-01-01 21:34:05Z`},
		{time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC), `10000-01-02 03:04:05.123456Z`},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), `0001-01-01 00:00:00Z BC`},

		// The BC leap day that rebuilding the year through time.Date used to move to
		// March 1.
		{time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC), `4713-02-29 00:00:00Z BC`},

		{time.Date(2024, 1, 2, 3, 4, 5, 999999999, time.UTC), `2024-01-02 03:04:05.999999Z`},
	} {
		buf, err := m.Encode(pgtype.TimestamptzOID, pgtype.TextFormatCode, tt.src, nil)
		require.NoErrorf(t, err, "%v", tt.src)
		require.Equalf(t, tt.want, string(buf), "%v", tt.src)
	}
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestamptzDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestamptzCodec{}

	for _, src := range []string{
		`eeeee`,
		`0000-01-01 00:00:00+00`,
		`2024-02-30 00:00:00+00`,
		`2024-13-01 00:00:00+00`,
		`10001-02-29 00:00:00+00`,
		`4712-02-29 00:00:00+00 BC`,

		// Beyond PostgreSQL's MAX_TZDISP_HOUR of 15.
		`2024-01-02 03:04:05+16`,
		`2024-01-02 03:04:05-16`,

		// Outside PostgreSQL's range once the offset has been applied.
		`294277-01-01 00:00:00+00`,
		`294277-01-01 15:59:00+15:59`,
		`294276-12-31 23:59:59.999999-14`,
		`4714-11-23 23:59:59+00 BC`,
		`4714-11-24 00:00:00+14 BC`,
		`294276-12-31 23:59:59.9999995+00`,

		// PostgreSQL always writes an offset for timestamptz.
		`2024-01-02 03:04:05`,
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)
		err := plan.Scan([]byte(src), &tstz)
		require.Errorf(t, err, "%s", src)
	}
}

// TestTimestamptzTextAndBinaryScanAgree is the invariant the text path's time zone rule
// exists to hold: the same value scanned in either wire format produces the same
// time.Time, down to the location it reports.
func TestTimestamptzTextAndBinaryScanAgree(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for _, expr := range []string{
			`'2024-07-01 12:00:00-04'::timestamptz`,
			`'2024-01-01 12:00:00+05:30'::timestamptz`,
			`'1883-11-18 12:03:57-04:56:02'::timestamptz`,
			`'2024-01-02 03:04:05.123456+00'::timestamptz`,
		} {
			var binary, text time.Time

			err := conn.QueryRow(ctx, "select "+expr, pgx.QueryExecModeCacheStatement).Scan(&binary)
			require.NoErrorf(t, err, "%s", expr)

			err = conn.QueryRow(ctx, "select "+expr, pgx.QueryExecModeSimpleProtocol).Scan(&text)
			require.NoErrorf(t, err, "%s", expr)

			require.Truef(t, binary.Equal(text), "%s: binary %v, text %v", expr, binary, text)
			require.Equalf(t, binary.String(), text.String(), "%s", expr)
		}
	})
}

// TestTimestamptzTextRoundTrip sends values through a real server in the simple protocol,
// so that both the text encoder and the text parser have to agree with it.
func TestTimestamptzTextRoundTrip(t *testing.T) {
	skipCockroachDB(t, "Server does not support the full PostgreSQL timestamptz range")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for _, in := range []time.Time{
			time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
			time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC),
			time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC),
			time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC),
			time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC),
			time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
		} {
			var out time.Time
			err := conn.QueryRow(ctx, "select $1::timestamptz", pgx.QueryExecModeSimpleProtocol, in).Scan(&out)
			require.NoErrorf(t, err, "%v", in)
			require.Truef(t, in.Equal(out), "expected %v got %v", in, out)
		}
	})
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

// TestTimestamptzCodecScanBinaryRange pins the range limits on the binary scan path. See
// TestDateCodecScanBinaryRange for why the binary path checks them at all.
func TestTimestamptzCodecScanBinaryRange(t *testing.T) {
	c := &pgtype.TimestamptzCodec{}

	// PostgreSQL's MIN_TIMESTAMP and END_TIMESTAMP, from
	// src/include/datatype/timestamp.h. END_TIMESTAMP is exclusive.
	const (
		minTimestamp = -211813488000000000
		endTimestamp = 9223371331200000000
	)

	src := func(microsecSinceY2K int64) []byte {
		return binary.BigEndian.AppendUint64(nil, uint64(microsecSinceY2K))
	}

	for _, tt := range []struct {
		microsecSinceY2K int64
		want             time.Time
	}{
		{0, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{minTimestamp, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{endTimestamp - 1, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},
	} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.BinaryFormatCode, &tstz)

		require.NoErrorf(t, plan.Scan(src(tt.microsecSinceY2K), &tstz), "%d", tt.microsecSinceY2K)
		require.Truef(t, tstz.Valid, "%d", tt.microsecSinceY2K)

		// The binary path returns time.Local, so compare the instant rather than the
		// time.Time.
		require.Truef(t, tt.want.Equal(tstz.Time), "%d: got %v, want %v", tt.microsecSinceY2K, tstz.Time, tt.want)
	}

	// The infinities are the int64 extremes and are handled before the range check, so
	// these stop short of them.
	for _, microsecSinceY2K := range []int64{minTimestamp - 1, endTimestamp, -9223372036854775000, 9223372036854775000} {
		var tstz pgtype.Timestamptz
		plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.BinaryFormatCode, &tstz)

		require.Errorf(t, plan.Scan(src(microsecSinceY2K), &tstz), "%d", microsecSinceY2K)
	}
}
