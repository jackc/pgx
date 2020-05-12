package pgtype_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

func TestCompositeTypeSetAndGet(t *testing.T) {
	ct := pgtype.NewCompositeType("test", &pgtype.Text{}, &pgtype.Int4{})
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
		expected []interface{}
	}{
		{
			src:      []interface{}{"foo", int32(42)},
			expected: []interface{}{"foo", int32(42)},
		},
		{
			src:      []interface{}{nil, nil},
			expected: []interface{}{nil, nil},
		},
		{
			src:      []interface{}{&pgtype.Text{String: "hi", Status: pgtype.Present}, &pgtype.Int4{Int: 7, Status: pgtype.Present}},
			expected: []interface{}{"hi", int32(7)},
		},
	}

	for i, tt := range compatibleValuesTests {
		err := ct.Set(tt.src)
		assert.NoErrorf(t, err, "%d", i)
		assert.EqualValues(t, tt.expected, ct.Get())
	}
}

func TestCompositeTypeAssignTo(t *testing.T) {
	ct := pgtype.NewCompositeType("test", &pgtype.Text{}, &pgtype.Int4{})

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

	c := pgtype.NewCompositeType("mytype", &pgtype.Int4{}, &pgtype.Text{})
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: c, Name: "mytype", OID: oid})

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
