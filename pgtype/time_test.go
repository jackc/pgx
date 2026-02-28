package pgtype_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
)

func TestTimeCodec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "time", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Time{Microseconds: 0, Valid: true},
			Result: new(pgtype.Time),
			Test:   isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			Param:  pgtype.Time{Microseconds: 1, Valid: true},
			Result: new(pgtype.Time),
			Test:   isExpectedEq(pgtype.Time{Microseconds: 1, Valid: true}),
		},
		{
			Param:  pgtype.Time{Microseconds: 86_399_999_999, Valid: true},
			Result: new(pgtype.Time),
			Test:   isExpectedEq(pgtype.Time{Microseconds: 86_399_999_999, Valid: true}),
		},
		{
			Param:  pgtype.Time{Microseconds: 86_400_000_000, Valid: true},
			Result: new(pgtype.Time),
			Test:   isExpectedEq(pgtype.Time{Microseconds: 86_400_000_000, Valid: true}),
		},
		{
			Param:  time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			Result: new(pgtype.Time),
			Test:   isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			Param:  pgtype.Time{Microseconds: 0, Valid: true},
			Result: new(time.Time),
			Test:   isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{Param: pgtype.Time{}, Result: new(pgtype.Time), Test: isExpectedEq(pgtype.Time{})},
		{Param: nil, Result: new(pgtype.Time), Test: isExpectedEq(pgtype.Time{})},
	})
}

func TestTimeTextScanner(t *testing.T) {
	var pgTime pgtype.Time

	assert.NoError(t, pgTime.Scan("07:37:16"))
	assert.Equal(t, true, pgTime.Valid)
	assert.Equal(t, int64(7*time.Hour+37*time.Minute+16*time.Second), pgTime.Microseconds*int64(time.Microsecond))

	assert.NoError(t, pgTime.Scan("15:04:05"))
	assert.Equal(t, true, pgTime.Valid)
	assert.Equal(t, int64(15*time.Hour+4*time.Minute+5*time.Second), pgTime.Microseconds*int64(time.Microsecond))

	// parsing of fractional digits
	assert.NoError(t, pgTime.Scan("15:04:05.00"))
	assert.Equal(t, true, pgTime.Valid)
	assert.Equal(t, int64(15*time.Hour+4*time.Minute+5*time.Second), pgTime.Microseconds*int64(time.Microsecond))

	const mirco = "789123"
	const woFraction = int64(4*time.Hour + 5*time.Minute + 6*time.Second) // time without fraction
	for i := 0; i <= len(mirco); i++ {
		assert.NoError(t, pgTime.Scan("04:05:06."+mirco[:i]))
		assert.Equal(t, true, pgTime.Valid)

		frac, _ := strconv.ParseInt(mirco[:i], 10, 64)
		for k := i; k < 6; k++ {
			frac *= 10
		}
		assert.Equal(t, woFraction+frac*int64(time.Microsecond), pgTime.Microseconds*int64(time.Microsecond))
	}

	// parsing of too long fraction errors
	assert.Error(t, pgTime.Scan("04:05:06.7891234"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	// parsing of timetz errors
	assert.Error(t, pgTime.Scan("04:05:06.789-08"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	assert.Error(t, pgTime.Scan("04:05:06-08:00"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	// parsing of date errors
	assert.Error(t, pgTime.Scan("1997-12-17"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	// parsing of text errors
	assert.Error(t, pgTime.Scan("12345678"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	assert.Error(t, pgTime.Scan("12-34-56"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	assert.Error(t, pgTime.Scan("12:34-56"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)

	assert.Error(t, pgTime.Scan("12-34:56"))
	assert.Equal(t, false, pgTime.Valid)
	assert.Equal(t, int64(0), pgTime.Microseconds)
}
