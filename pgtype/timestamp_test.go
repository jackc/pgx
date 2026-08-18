package pgtype_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support infinite timestamps (see https://github.com/cockroachdb/cockroach/issues/41564)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamp", []pgxtest.ValueRoundTripTest{
		{Param: time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},

		{Param: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC))},

		// Nanosecond truncation
		{Param: time.Date(2020, 1, 1, 0, 0, 0, 999999999, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.UTC))},
		{Param: time.Date(2020, 1, 1, 0, 0, 0, 999999001, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2020, 1, 1, 0, 0, 0, 999999000, time.UTC))},

		{Param: pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}, Result: new(pgtype.Timestamp), Test: isExpectedEq(pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true})},
		{Param: pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, Result: new(pgtype.Timestamp), Test: isExpectedEq(pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{Param: pgtype.Timestamp{}, Result: new(pgtype.Timestamp), Test: isExpectedEq(pgtype.Timestamp{})},
		{Param: nil, Result: new(*time.Time), Test: isExpectedEq((*time.Time)(nil))},
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
		{Param: pgtype.Timestamp{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Valid: true}, Result: new(time.Time), Test: isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
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
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local))},
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

func TestTimestampCodecDecodeText(t *testing.T) {
	c := &pgtype.TimestampCodec{}

	for _, tt := range []struct {
		src  string
		want time.Time
	}{
		{`2024-01-02 03:04:05.123456`, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{`2024-01-02 03:04:05`, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{`2024-01-02 03:04:05.5`, time.Date(2024, 1, 2, 3, 4, 5, 500000000, time.UTC)},

		// Years past four digits, which the previous layout based parser could not read at
		// all.
		{`10000-01-02 03:04:05.123456`, time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{`294276-12-31 23:59:59.999999`, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},

		// BC years. 4713-02-29 BC is astronomical year -4712, which is a leap year, so the
		// date exists; parsing it as an AD year did not.
		{`0001-01-01 00:00:00 BC`, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{`4713-02-29 00:00:00 BC`, time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC)},
		{`4714-11-24 00:00:00 BC`, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},

		// The fraction is rounded to microseconds before the range is checked, so this
		// lands exactly on the maximum rather than past it.
		{`294276-12-31 23:59:59.9999994`, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},
	} {
		var ts pgtype.Timestamp
		plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, &ts)

		err := plan.Scan([]byte(tt.src), &ts)
		require.NoErrorf(t, err, "%s", tt.src)
		require.Truef(t, ts.Valid, "%s", tt.src)
		require.Equalf(t, tt.want, ts.Time, "%s", tt.src)
	}
}

func TestTimestampCodecEncodeText(t *testing.T) {
	m := pgtype.NewMap()

	for _, tt := range []struct {
		src  time.Time
		want string
	}{
		{time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC), `2024-01-02 03:04:05.123456`},
		{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), `2024-01-02 03:04:05`},
		{time.Date(2024, 1, 2, 3, 4, 5, 500000000, time.UTC), `2024-01-02 03:04:05.5`},
		{time.Date(10000, 1, 2, 3, 4, 5, 123456000, time.UTC), `10000-01-02 03:04:05.123456`},
		{time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), `294276-12-31 23:59:59.999999`},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), `0001-01-01 00:00:00 BC`},

		// Rebuilding the year through time.Date used to roll February 29 of a BC leap year
		// forward to March 1, silently changing the value.
		{time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC), `4713-02-29 00:00:00 BC`},
		{time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC), `4714-11-24 00:00:00 BC`},

		// Sub-microsecond precision does not survive the wire format.
		{time.Date(2024, 1, 2, 3, 4, 5, 999999999, time.UTC), `2024-01-02 03:04:05.999999`},
	} {
		buf, err := m.Encode(pgtype.TimestampOID, pgtype.TextFormatCode, tt.src, nil)
		require.NoErrorf(t, err, "%v", tt.src)
		require.Equalf(t, tt.want, string(buf), "%v", tt.src)
	}
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestampCodecDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestampCodec{}

	for _, src := range []string{
		`eeeee`,
		`0000-01-01 00:00:00`,
		`2024-02-30 00:00:00`,
		`2024-13-01 00:00:00`,
		`10000-02-30 00:00:00`,
		`10001-02-29 00:00:00`,
		`4712-02-29 00:00:00 BC`,

		// Outside PostgreSQL's range for timestamp.
		`294277-01-01 00:00:00`,
		`4714-11-23 23:59:59 BC`,
		`9223372036854775808-01-01 00:00:00`,

		// Rounding the fraction carries this past the maximum.
		`294276-12-31 23:59:59.9999995`,

		// A time zone offset makes it a timestamptz, not a timestamp.
		`2024-01-02 03:04:05+00`,
	} {
		var ts pgtype.Timestamp
		plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, &ts)
		err := plan.Scan([]byte(src), &ts)
		require.Errorf(t, err, "%s", src)
	}
}

// TestTimestampTextRoundTrip sends values through a real server in the simple protocol, so
// that both the text encoder and the text parser have to agree with it rather than only
// with each other.
func TestTimestampTextRoundTrip(t *testing.T) {
	skipCockroachDB(t, "Server does not support the full PostgreSQL timestamp range")

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
			err := conn.QueryRow(ctx, "select $1::timestamp", pgx.QueryExecModeSimpleProtocol, in).Scan(&out)
			require.NoErrorf(t, err, "%v", in)
			require.Truef(t, in.Equal(out), "expected %v got %v", in, out)
		}
	})
}

func TestTimestampMarshalJSON(t *testing.T) {
	tsStruct := struct {
		TS pgtype.Timestamp `json:"ts"`
	}{}

	tm := time.Date(2012, 3, 29, 10, 5, 45, 0, time.UTC)
	tsString := "\"" + tm.Format("2006-01-02T15:04:05") + "\"" //  `"2012-03-29T10:05:45"`
	var pgt pgtype.Timestamp
	_ = pgt.Scan(tm)

	successfulTests := []struct {
		source pgtype.Timestamp
		result string
	}{
		{source: pgtype.Timestamp{}, result: "null"},
		{source: pgtype.Timestamp{Time: tm, Valid: true}, result: tsString},
		{source: pgt, result: tsString},
		{source: pgtype.Timestamp{Time: tm.Add(time.Second * 555 / 1000), Valid: true}, result: `"2012-03-29T10:05:45.555"`},
		{source: pgtype.Timestamp{InfinityModifier: pgtype.Infinity, Valid: true}, result: "\"infinity\""},
		{source: pgtype.Timestamp{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, result: "\"-infinity\""},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !assert.Equal(t, tt.result, string(r)) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
		tsStruct.TS = tt.source
		b, err := json.Marshal(tsStruct)
		assert.NoErrorf(t, err, "failed to marshal %v %s", tt.source, err)
		t2 := tsStruct
		t2.TS = pgtype.Timestamp{} // Clear out the value so that we can compare after unmarshalling
		err = json.Unmarshal(b, &t2)
		assert.NoErrorf(t, err, "failed to unmarshal %v with %s", tt.source, err)
		assert.True(t, tsStruct.TS.Time.Unix() == t2.TS.Time.Unix())
	}
}

func TestTimestampUnmarshalJSONErrors(t *testing.T) {
	tsStruct := struct {
		TS pgtype.Timestamp `json:"ts"`
	}{}
	goodJson1 := []byte(`{"ts":"2012-03-29T10:05:45"}`)
	assert.NoError(t, json.Unmarshal(goodJson1, &tsStruct))
	goodJson2 := []byte(`{"ts":"2012-03-29T10:05:45Z"}`)
	assert.NoError(t, json.Unmarshal(goodJson2, &tsStruct))
	badJson := []byte(`{"ts":"2012-03-29"}`)
	assert.Error(t, json.Unmarshal(badJson, &tsStruct))
}

func TestTimestampUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Timestamp
	}{
		{source: "null", result: pgtype.Timestamp{}},
		{source: "\"2012-03-29T10:05:45\"", result: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.UTC), Valid: true}},
		{source: "\"2012-03-29T10:05:45.555\"", result: pgtype.Timestamp{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.UTC), Valid: true}},
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

// TestTimestampCodecScanBinaryRange pins the range limits on the binary scan path. See
// TestDateCodecScanBinaryRange for why the binary path checks them at all.
func TestTimestampCodecScanBinaryRange(t *testing.T) {
	c := &pgtype.TimestampCodec{}

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
		var ts pgtype.Timestamp
		plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.BinaryFormatCode, &ts)

		require.NoErrorf(t, plan.Scan(src(tt.microsecSinceY2K), &ts), "%d", tt.microsecSinceY2K)
		require.Truef(t, ts.Valid, "%d", tt.microsecSinceY2K)
		require.Equalf(t, tt.want, ts.Time, "%d", tt.microsecSinceY2K)
	}

	// The infinities are the int64 extremes and are handled before the range check, so
	// these stop short of them.
	for _, microsecSinceY2K := range []int64{minTimestamp - 1, endTimestamp, -9223372036854775000, 9223372036854775000} {
		var ts pgtype.Timestamp
		plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.BinaryFormatCode, &ts)

		require.Errorf(t, plan.Scan(src(microsecSinceY2K), &ts), "%d", microsecSinceY2K)
	}
}
