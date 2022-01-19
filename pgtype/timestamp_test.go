package pgtype_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestTimestampCodec(t *testing.T) {
	testPgxCodec(t, "timestamp", []PgxTranscodeTestCase{
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

// https://github.com/jackc/pgx/v4/pgtype/pull/128
func TestTimestampTranscodeBigTimeBinary(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	in := &pgtype.Timestamp{Time: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC), Valid: true}
	var out pgtype.Timestamp

	err := conn.QueryRow(context.Background(), "select $1::timestamp", in).Scan(&out)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, in.Valid, out.Valid)
	require.Truef(t, in.Time.Equal(out.Time), "expected %v got %v", in.Time, out.Time)
}

// https://github.com/jackc/pgtype/issues/74
func TestTimestampCodecDecodeTextInvalid(t *testing.T) {
	c := &pgtype.TimestampCodec{}
	var ts pgtype.Timestamp
	plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, &ts, false)
	err := plan.Scan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, []byte(`eeeee`), &ts)
	require.Error(t, err)
}
