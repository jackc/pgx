package pgtype_test

import (
	"context"
	"fmt"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestCompositeCodecTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists ct_test;

create type ct_test as (
	a text,
  b int4
);`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type ct_test")

	dt, err := conn.LoadDataType(context.Background(), "ct_test")
	require.NoError(t, err)
	conn.ConnInfo().RegisterDataType(*dt)

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

		err := conn.QueryRow(context.Background(), "select $1::ct_test", pgx.QueryResultFormats{format.code},
			pgtype.CompositeFields{"hi", int32(42)},
		).Scan(
			pgtype.CompositeFields{&a, &b},
		)
		require.NoErrorf(t, err, "%v", format.name)
		require.EqualValuesf(t, "hi", a, "%v", format.name)
		require.EqualValuesf(t, 42, b, "%v", format.name)
	}
}

type point3d struct {
	X, Y, Z float64
}

func (p point3d) IsNull() bool {
	return false
}

func (p point3d) Index(i int) interface{} {
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

func (p *point3d) ScanIndex(i int) interface{} {
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
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists point3d;

create type point3d as (
	x float8,
	y float8,
	z float8
);`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type point3d")

	dt, err := conn.LoadDataType(context.Background(), "point3d")
	require.NoError(t, err)
	conn.ConnInfo().RegisterDataType(*dt)

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
		err := conn.QueryRow(context.Background(), "select $1::point3d", pgx.QueryResultFormats{format.code}, input).Scan(&output)
		require.NoErrorf(t, err, "%v", format.name)
		require.Equalf(t, input, output, "%v", format.name)
	}
}
