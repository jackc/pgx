package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

func TestCompositeFieldsDecode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	formats := []int16{pgx.TextFormatCode, pgx.BinaryFormatCode}

	// Assorted values
	{
		var a int32
		var b string
		var c float64

		for _, format := range formats {
			err := conn.QueryRow(context.Background(), "select row(1,'hi',2.1)", pgx.QueryResultFormats{format}).Scan(
				pgtype.CompositeFields{&a, &b, &c},
			)
			if !assert.NoErrorf(t, err, "Format: %v", format) {
				continue
			}

			assert.EqualValuesf(t, 1, a, "Format: %v", format)
			assert.EqualValuesf(t, "hi", b, "Format: %v", format)
			assert.EqualValuesf(t, 2.1, c, "Format: %v", format)
		}
	}

	// nulls, string "null", and empty string fields
	{
		var a pgtype.Text
		var b string
		var c pgtype.Text
		var d string
		var e pgtype.Text

		for _, format := range formats {
			err := conn.QueryRow(context.Background(), "select row(null,'null',null,'',null)", pgx.QueryResultFormats{format}).Scan(
				pgtype.CompositeFields{&a, &b, &c, &d, &e},
			)
			if !assert.NoErrorf(t, err, "Format: %v", format) {
				continue
			}

			assert.Nilf(t, a.Get(), "Format: %v", format)
			assert.EqualValuesf(t, "null", b, "Format: %v", format)
			assert.Nilf(t, c.Get(), "Format: %v", format)
			assert.EqualValuesf(t, "", d, "Format: %v", format)
			assert.Nilf(t, e.Get(), "Format: %v", format)
		}
	}

	// null record
	{
		var a pgtype.Text
		var b string
		cf := pgtype.CompositeFields{&a, &b}

		for _, format := range formats {
			// Cannot scan nil into
			err := conn.QueryRow(context.Background(), "select null::record", pgx.QueryResultFormats{format}).Scan(
				cf,
			)
			if assert.Errorf(t, err, "Format: %v", format) {
				continue
			}
			assert.NotNilf(t, cf, "Format: %v", format)

			// But can scan nil into *pgtype.CompositeFields
			err = conn.QueryRow(context.Background(), "select null::record", pgx.QueryResultFormats{format}).Scan(
				&cf,
			)
			if assert.Errorf(t, err, "Format: %v", format) {
				continue
			}
			assert.Nilf(t, cf, "Format: %v", format)
		}
	}

	// quotes and special characters
	{
		var a, b, c, d string

		for _, format := range formats {
			err := conn.QueryRow(context.Background(), `select row('"', 'foo bar', 'foo''bar', 'baz)bar')`, pgx.QueryResultFormats{format}).Scan(
				pgtype.CompositeFields{&a, &b, &c, &d},
			)
			if !assert.NoErrorf(t, err, "Format: %v", format) {
				continue
			}

			assert.Equalf(t, `"`, a, "Format: %v", format)
			assert.Equalf(t, `foo bar`, b, "Format: %v", format)
			assert.Equalf(t, `foo'bar`, c, "Format: %v", format)
			assert.Equalf(t, `baz)bar`, d, "Format: %v", format)
		}
	}

	// arrays
	{
		var a []string
		var b []int64

		for _, format := range formats {
			err := conn.QueryRow(context.Background(), `select row(array['foo', 'bar', 'baz'], array[1,2,3])`, pgx.QueryResultFormats{format}).Scan(
				pgtype.CompositeFields{&a, &b},
			)
			if !assert.NoErrorf(t, err, "Format: %v", format) {
				continue
			}

			assert.EqualValuesf(t, []string{"foo", "bar", "baz"}, a, "Format: %v", format)
			assert.EqualValuesf(t, []int64{1, 2, 3}, b, "Format: %v", format)
		}
	}
}
