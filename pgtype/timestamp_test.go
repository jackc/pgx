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

func TestTimestampCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamp", []pgxtest.ValueRoundTripTest{
		{time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},

		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC))},
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC))},
		{time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEqTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC))},

		// Nanosecond truncation
		{time.Date(2020, 1, 1, 0, 0, 0, 999999999, time.UTC), new(time.Time), isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.UTC))},
		{time.Date(2020, 1, 1, 0, 0, 0, 999999001, time.UTC), new(time.Time), isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.UTC))},

		{pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}, new(pgtype.Timestamp), isExpectedEq(pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true})},
		{pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, new(pgtype.Timestamp), isExpectedEq(pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{pgtype.Timestamp{}, new(pgtype.Timestamp), isExpectedEq(pgtype.Timestamp{})},
		{nil, new(*time.Time), isExpectedEq((*time.Time)(nil))},
	})
}

func TestTimestampCodecWithScanLocationUTC(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name:  "timestamp",
			OID:   pgtype.TimestampOID,
			Codec: &pgtype.TimestampCodec{ScanLocation: time.UTC},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, nil, "timestamp", []pgxtest.ValueRoundTripTest{
		// Have to use pgtype.Timestamp instead of time.Time as source because otherwise the simple and exec query exec
		// modes will encode the time for timestamptz. That is, they will convert it from local time zone.
		{pgtype.Timestamp{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Valid: true}, new(time.Time), isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
	})
}

func TestTimestampCodecWithScanLocationLocal(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	connTestRunner := defaultConnTestRunner
	connTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		conn.TypeMap().RegisterType(&pgtype.Type{
			Name:  "timestamp",
			OID:   pgtype.TimestampOID,
			Codec: &pgtype.TimestampCodec{ScanLocation: time.Local},
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, connTestRunner, nil, "timestamp", []pgxtest.ValueRoundTripTest{
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), new(time.Time), isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local))},
	})
}

// https://github.com/jackc/pgx/v4/pgtype/pull/128
func TestTimestampTranscodeBigTimeBinary(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		in := &pgtype.Timestamp{Time: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), Valid: true}
		var out pgtype.Timestamp

		err := conn.QueryRow(ctx, "select $1::timestamp", in).Scan(&out)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, in.Valid, out.Valid)
		require.Truef(t, in.Time.Equal(out.Time), "expected %v got %v", in.Time, out.Time)
	})
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestampCodecDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestampCodec{}
	var ts pgtype.Timestamp
	plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, &ts)
	err := plan.Scan([]byte(`eeeee`), &ts)
	require.Error(t, err)
}

func TestTimestampMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Timestamp
		result string
	}{
		{source: pgtype.Timestamp{}, result: "null"},
		{source: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.UTC), Valid: true}, result: "\"2012-03-29T10:05:45Z\""},
		{source: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.UTC), Valid: true}, result: "\"2012-03-29T10:05:45.555Z\""},
		{source: pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}, result: "\"infinity\""},
		{source: pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, result: "\"-infinity\""},
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

func TestTimestampUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Timestamp
	}{
		{source: "null", result: pgtype.Timestamp{}},
		{source: "\"2012-03-29T10:05:45Z\"", result: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.UTC), Valid: true}},
		{source: "\"2012-03-29T10:05:45.555Z\"", result: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.UTC), Valid: true}},
		{source: "\"infinity\"", result: pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}},
		{source: "\"-infinity\"", result: pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Timestamp
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !r.Time.Equal(tt.result.Time) || r.Valid != tt.result.Valid || r.InfinityModifier != tt.result.InfinityModifier {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
