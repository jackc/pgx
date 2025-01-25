package pgtype_test

import (
	"context"
	"fmt"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestCompositeCodecTranscode(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists ct_test;

create type ct_test as (
	a text,
  b int4
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type ct_test")

		dt, err := conn.LoadType(ctx, "ct_test")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		formats := []struct {
			name string
			code int16
		}{
			{name: "TextFormat", code: pgx.TextFormatCode},
			{name: "BinaryFormat", code: pgx.BinaryFormatCode},
		}

		for _, format := range formats {
			var a string
			var b int32

			err := conn.QueryRow(ctx, "select $1::ct_test", pgx.QueryResultFormats{format.code},
				pgtype.CompositeFields{"hi", int32(42)},
			).Scan(
				pgtype.CompositeFields{&a, &b},
			)
			require.NoErrorf(t, err, "%v", format.name)
			require.EqualValuesf(t, "hi", a, "%v", format.name)
			require.EqualValuesf(t, 42, b, "%v", format.name)
		}
	})
}

type point3d struct {
	X, Y, Z float64
}

func (p point3d) IsNull() bool {
	return false
}

func (p point3d) Index(i int) any {
	switch i {
	case 0:
		return p.X
	case 1:
		return p.Y
	case 2:
		return p.Z
	default:
		panic("invalid index")
	}
}

func (p *point3d) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into point3d")
}

func (p *point3d) ScanIndex(i int) any {
	switch i {
	case 0:
		return &p.X
	case 1:
		return &p.Y
	case 2:
		return &p.Z
	default:
		panic("invalid index")
	}
}

func TestCompositeCodecTranscodeStruct(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists point3d;

create type point3d as (
	x float8,
	y float8,
	z float8
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type point3d")

		dt, err := conn.LoadType(ctx, "point3d")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		formats := []struct {
			name string
			code int16
		}{
			{name: "TextFormat", code: pgx.TextFormatCode},
			{name: "BinaryFormat", code: pgx.BinaryFormatCode},
		}

		for _, format := range formats {
			input := point3d{X: 1, Y: 2, Z: 3}
			var output point3d
			err := conn.QueryRow(ctx, "select $1::point3d", pgx.QueryResultFormats{format.code}, input).Scan(&output)
			require.NoErrorf(t, err, "%v", format.name)
			require.Equalf(t, input, output, "%v", format.name)
		}
	})
}

func TestCompositeCodecTranscodeStructWrapper(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists point3d;

create type point3d as (
	x float8,
	y float8,
	z float8
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type point3d")

		dt, err := conn.LoadType(ctx, "point3d")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		formats := []struct {
			name string
			code int16
		}{
			{name: "TextFormat", code: pgx.TextFormatCode},
			{name: "BinaryFormat", code: pgx.BinaryFormatCode},
		}

		type anotherPoint struct {
			X, Y, Z float64
		}

		for _, format := range formats {
			input := anotherPoint{X: 1, Y: 2, Z: 3}
			var output anotherPoint
			err := conn.QueryRow(ctx, "select $1::point3d", pgx.QueryResultFormats{format.code}, input).Scan(&output)
			require.NoErrorf(t, err, "%v", format.name)
			require.Equalf(t, input, output, "%v", format.name)
		}
	})
}

func TestCompositeCodecDecodeValue(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists point3d;

create type point3d as (
	x float8,
	y float8,
	z float8
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type point3d")

		dt, err := conn.LoadType(ctx, "point3d")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		formats := []struct {
			name string
			code int16
		}{
			{name: "TextFormat", code: pgx.TextFormatCode},
			{name: "BinaryFormat", code: pgx.BinaryFormatCode},
		}

		for _, format := range formats {
			rows, err := conn.Query(ctx, "select '(1,2,3)'::point3d", pgx.QueryResultFormats{format.code})
			require.NoErrorf(t, err, "%v", format.name)
			require.True(t, rows.Next())
			values, err := rows.Values()
			require.NoErrorf(t, err, "%v", format.name)
			require.Lenf(t, values, 1, "%v", format.name)
			require.Equalf(t, map[string]any{"x": 1.0, "y": 2.0, "z": 3.0}, values[0], "%v", format.name)
			require.False(t, rows.Next())
			require.NoErrorf(t, rows.Err(), "%v", format.name)
		}
	})
}

// Test for composite type from table instead of create type. Table types have system / hidden columns like tableoid,
// cmax, xmax, etc. These are not included when sending or receiving composite types.
//
// https://github.com/jackc/pgx/issues/1576
func TestCompositeCodecTranscodeStructWrapperForTable(t *testing.T) {
	skipCockroachDB(t, "Server does not support composite types from table definitions")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop table if exists point3d;

create table point3d (
	x float8,
	y float8,
	z float8
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop table point3d")

		dt, err := conn.LoadType(ctx, "point3d")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		formats := []struct {
			name string
			code int16
		}{
			{name: "TextFormat", code: pgx.TextFormatCode},
			{name: "BinaryFormat", code: pgx.BinaryFormatCode},
		}

		type anotherPoint struct {
			X, Y, Z float64
		}

		for _, format := range formats {
			input := anotherPoint{X: 1, Y: 2, Z: 3}
			var output anotherPoint
			err := conn.QueryRow(ctx, "select $1::point3d", pgx.QueryResultFormats{format.code}, input).Scan(&output)
			require.NoErrorf(t, err, "%v", format.name)
			require.Equalf(t, input, output, "%v", format.name)
		}
	})
}
