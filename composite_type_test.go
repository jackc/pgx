package pgtype_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	pgx "github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeTypeSetAndGet(t *testing.T) {
	ci := pgtype.NewConnInfo()
	ct, err := pgtype.NewCompositeType("test", []pgtype.CompositeTypeField{
		{"a", pgtype.TextOID},
		{"b", pgtype.Int4OID},
	}, ci)
	require.NoError(t, err)
	assert.Equal(t, pgtype.Undefined, ct.Get())

	nilTests := []struct {
		src interface{}
	}{
		{nil},                   // nil interface
		{(*[]interface{})(nil)}, // typed nil
	}

	for i, tt := range nilTests {
		err := ct.Set(tt.src)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equal(t, nil, ct.Get())
	}

	compatibleValuesTests := []struct {
		src      []interface{}
		expected map[string]interface{}
	}{
		{
			src:      []interface{}{"foo", int32(42)},
			expected: map[string]interface{}{"a": "foo", "b": int32(42)},
		},
		{
			src:      []interface{}{nil, nil},
			expected: map[string]interface{}{"a": nil, "b": nil},
		},
		{
			src:      []interface{}{&pgtype.Text{String: "hi", Status: pgtype.Present}, &pgtype.Int4{Int: 7, Status: pgtype.Present}},
			expected: map[string]interface{}{"a": "hi", "b": int32(7)},
		},
	}

	for i, tt := range compatibleValuesTests {
		err := ct.Set(tt.src)
		assert.NoErrorf(t, err, "%d", i)
		assert.EqualValues(t, tt.expected, ct.Get())
	}
}

func TestCompositeTypeAssignTo(t *testing.T) {
	ci := pgtype.NewConnInfo()
	ct, err := pgtype.NewCompositeType("test", []pgtype.CompositeTypeField{
		{"a", pgtype.TextOID},
		{"b", pgtype.Int4OID},
	}, ci)
	require.NoError(t, err)

	{
		err := ct.Set([]interface{}{"foo", int32(42)})
		assert.NoError(t, err)

		var a string
		var b int32

		err = ct.AssignTo([]interface{}{&a, &b})
		assert.NoError(t, err)

		assert.Equal(t, "foo", a)
		assert.Equal(t, int32(42), b)
	}

	{
		err := ct.Set([]interface{}{"foo", int32(42)})
		assert.NoError(t, err)

		var a pgtype.Text
		var b pgtype.Int4

		err = ct.AssignTo([]interface{}{&a, &b})
		assert.NoError(t, err)

		assert.Equal(t, pgtype.Text{String: "foo", Status: pgtype.Present}, a)
		assert.Equal(t, pgtype.Int4{Int: 42, Status: pgtype.Present}, b)
	}

	// Allow nil destination component as no-op
	{
		err := ct.Set([]interface{}{"foo", int32(42)})
		assert.NoError(t, err)

		var b int32

		err = ct.AssignTo([]interface{}{nil, &b})
		assert.NoError(t, err)

		assert.Equal(t, int32(42), b)
	}

	// *[]interface{} dest when null
	{
		err := ct.Set(nil)
		assert.NoError(t, err)

		var a pgtype.Text
		var b pgtype.Int4
		dst := []interface{}{&a, &b}

		err = ct.AssignTo(&dst)
		assert.NoError(t, err)

		assert.Nil(t, dst)
	}

	// *[]interface{} dest when not null
	{
		err := ct.Set([]interface{}{"foo", int32(42)})
		assert.NoError(t, err)

		var a pgtype.Text
		var b pgtype.Int4
		dst := []interface{}{&a, &b}

		err = ct.AssignTo(&dst)
		assert.NoError(t, err)

		assert.NotNil(t, dst)
		assert.Equal(t, pgtype.Text{String: "foo", Status: pgtype.Present}, a)
		assert.Equal(t, pgtype.Int4{Int: 42, Status: pgtype.Present}, b)
	}

	// Struct fields positionally via reflection
	{
		err := ct.Set([]interface{}{"foo", int32(42)})
		assert.NoError(t, err)

		s := struct {
			A string
			B int32
		}{}

		err = ct.AssignTo(&s)
		if assert.NoError(t, err) {
			assert.Equal(t, "foo", s.A)
			assert.Equal(t, int32(42), s.B)
		}
	}
}

func TestCompositeTypeTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists ct_test;

create type ct_test as (
	a text,
  b int4
);`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type ct_test")

	var oid uint32
	err = conn.QueryRow(context.Background(), `select 'ct_test'::regtype::oid`).Scan(&oid)
	require.NoError(t, err)

	defer conn.Exec(context.Background(), "drop type ct_test")

	ct, err := pgtype.NewCompositeType("ct_test", []pgtype.CompositeTypeField{
		{"a", pgtype.TextOID},
		{"b", pgtype.Int4OID},
	}, conn.ConnInfo())
	require.NoError(t, err)
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: ct, Name: ct.TypeName(), OID: oid})

	// Use simple protocol to force text or binary encoding
	simpleProtocols := []bool{true, false}

	var a string
	var b int32

	for _, simpleProtocol := range simpleProtocols {
		err := conn.QueryRow(context.Background(), "select $1::ct_test", pgx.QuerySimpleProtocol(simpleProtocol),
			pgtype.CompositeFields{"hi", int32(42)},
		).Scan(
			[]interface{}{&a, &b},
		)
		if assert.NoErrorf(t, err, "Simple Protocol: %v", simpleProtocol) {
			assert.EqualValuesf(t, "hi", a, "Simple Protocol: %v", simpleProtocol)
			assert.EqualValuesf(t, 42, b, "Simple Protocol: %v", simpleProtocol)
		}
	}
}

func Example_composite() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), `drop type if exists mytype;`)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = conn.Exec(context.Background(), `create type mytype as (
  a int4,
  b text
);`)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Exec(context.Background(), "drop type mytype")

	var oid uint32
	err = conn.QueryRow(context.Background(), `select 'mytype'::regtype::oid`).Scan(&oid)
	if err != nil {
		fmt.Println(err)
		return
	}

	ct, err := pgtype.NewCompositeType("mytype", []pgtype.CompositeTypeField{
		{"a", pgtype.Int4OID},
		{"b", pgtype.TextOID},
	}, conn.ConnInfo())
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: ct, Name: ct.TypeName(), OID: oid})

	var a int
	var b *string

	err = conn.QueryRow(context.Background(), "select $1::mytype", []interface{}{2, "bar"}).Scan([]interface{}{&a, &b})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("First: a=%d b=%s\n", a, *b)

	err = conn.QueryRow(context.Background(), "select (1, NULL)::mytype").Scan([]interface{}{&a, &b})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Second: a=%d b=%v\n", a, b)

	scanTarget := []interface{}{&a, &b}
	err = conn.QueryRow(context.Background(), "select NULL::mytype").Scan(&scanTarget)
	E(err)

	fmt.Printf("Third: isNull=%v\n", scanTarget == nil)

	// Output:
	// First: a=2 b=bar
	// Second: a=1 b=<nil>
	// Third: isNull=true
}
