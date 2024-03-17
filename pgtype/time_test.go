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
			pgtype.Time{Microseconds: 0, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 1, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 1, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 86399999999, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 86399999999, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 86400000000, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 86400000000, Valid: true}),
		},
		{
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 0, Valid: true},
			new(time.Time),
			isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{pgtype.Time{}, new(pgtype.Time), isExpectedEq(pgtype.Time{})},
		{nil, new(pgtype.Time), isExpectedEq(pgtype.Time{})},
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
