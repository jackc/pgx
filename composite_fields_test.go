package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	// Skip nil fields
	{
		var a int32
		var c float64

		for _, format := range formats {
			err := conn.QueryRow(context.Background(), "select row(1,'hi',2.1)", pgx.QueryResultFormats{format}).Scan(
				pgtype.CompositeFields{&a, nil, &c},
			)
			if !assert.NoErrorf(t, err, "Format: %v", format) {
				continue
			}

			assert.EqualValuesf(t, 1, a, "Format: %v", format)
			assert.EqualValuesf(t, 2.1, c, "Format: %v", format)
		}
	}
}

func TestCompositeFieldsEncode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists cf_encode;

create type cf_encode as (
	a text,
  b int4,
	c text,
	d float8,
	e text
);`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type cf_encode")

	// Use simple protocol to force text or binary encoding
	simpleProtocols := []bool{true, false}

	// Assorted values
	{
		var a string
		var b int32
		var c string
		var d float64
		var e string

		for _, simpleProtocol := range simpleProtocols {
			err := conn.QueryRow(context.Background(), "select $1::cf_encode", pgx.QuerySimpleProtocol(simpleProtocol),
				pgtype.CompositeFields{"hi", int32(1), "ok", float64(2.1), "bye"},
			).Scan(
				pgtype.CompositeFields{&a, &b, &c, &d, &e},
			)
			if assert.NoErrorf(t, err, "Simple Protocol: %v", simpleProtocol) {
				assert.EqualValuesf(t, "hi", a, "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, 1, b, "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, "ok", c, "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, 2.1, d, "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, "bye", e, "Simple Protocol: %v", simpleProtocol)
			}
		}
	}

	// untyped nil
	{
		var a pgtype.Text
		var b int32
		var c string
		var d pgtype.Float8
		var e pgtype.Text

		simpleProtocol := true
		err := conn.QueryRow(context.Background(), "select $1::cf_encode", pgx.QuerySimpleProtocol(simpleProtocol),
			pgtype.CompositeFields{nil, int32(1), "null", nil, nil},
		).Scan(
			pgtype.CompositeFields{&a, &b, &c, &d, &e},
		)
		if assert.NoErrorf(t, err, "Simple Protocol: %v", simpleProtocol) {
			assert.Nilf(t, a.Get(), "Simple Protocol: %v", simpleProtocol)
			assert.EqualValuesf(t, 1, b, "Simple Protocol: %v", simpleProtocol)
			assert.EqualValuesf(t, "null", c, "Simple Protocol: %v", simpleProtocol)
			assert.Nilf(t, d.Get(), "Simple Protocol: %v", simpleProtocol)
			assert.Nilf(t, e.Get(), "Simple Protocol: %v", simpleProtocol)
		}

		// untyped nil cannot be represented in binary format because CompositeFields does not know the PostgreSQL schema
		// of the composite type.
		simpleProtocol = false
		err = conn.QueryRow(context.Background(), "select $1::cf_encode", pgx.QuerySimpleProtocol(simpleProtocol),
			pgtype.CompositeFields{nil, int32(1), "null", nil, nil},
		).Scan(
			pgtype.CompositeFields{&a, &b, &c, &d, &e},
		)
		assert.Errorf(t, err, "Simple Protocol: %v", simpleProtocol)
	}

	// nulls, string "null", and empty string fields
	{
		var a pgtype.Text
		var b int32
		var c string
		var d pgtype.Float8
		var e pgtype.Text

		for _, simpleProtocol := range simpleProtocols {
			err := conn.QueryRow(context.Background(), "select $1::cf_encode", pgx.QuerySimpleProtocol(simpleProtocol),
				pgtype.CompositeFields{&pgtype.Text{Status: pgtype.Null}, int32(1), "null", &pgtype.Float8{Status: pgtype.Null}, &pgtype.Text{Status: pgtype.Null}},
			).Scan(
				pgtype.CompositeFields{&a, &b, &c, &d, &e},
			)
			if assert.NoErrorf(t, err, "Simple Protocol: %v", simpleProtocol) {
				assert.Nilf(t, a.Get(), "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, 1, b, "Simple Protocol: %v", simpleProtocol)
				assert.EqualValuesf(t, "null", c, "Simple Protocol: %v", simpleProtocol)
				assert.Nilf(t, d.Get(), "Simple Protocol: %v", simpleProtocol)
				assert.Nilf(t, e.Get(), "Simple Protocol: %v", simpleProtocol)
			}
		}
	}

	// quotes and special characters
	{
		var a string
		var b int32
		var c string
		var d float64
		var e string

		for _, simpleProtocol := range simpleProtocols {
			err := conn.QueryRow(
				context.Background(),
				`select $1::cf_encode`,
				pgx.QuerySimpleProtocol(simpleProtocol),
				pgtype.CompositeFields{`"`, int32(42), `foo'bar`, float64(1.2), `baz)bar`},
			).Scan(
				pgtype.CompositeFields{&a, &b, &c, &d, &e},
			)
			if assert.NoErrorf(t, err, "Simple Protocol: %v", simpleProtocol) {
				assert.Equalf(t, `"`, a, "Simple Protocol: %v", simpleProtocol)
				assert.Equalf(t, int32(42), b, "Simple Protocol: %v", simpleProtocol)
				assert.Equalf(t, `foo'bar`, c, "Simple Protocol: %v", simpleProtocol)
				assert.Equalf(t, float64(1.2), d, "Simple Protocol: %v", simpleProtocol)
				assert.Equalf(t, `baz)bar`, e, "Simple Protocol: %v", simpleProtocol)
			}
		}
	}
}
