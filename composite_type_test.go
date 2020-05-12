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
	ct := pgtype.NewCompositeType(&pgtype.Text{}, &pgtype.Int4{})
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

//ExampleComposite demonstrates use of Row() function to pass and receive
// back composite types without creating boilderplate custom types.
func Example_composite() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	E(err)

	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), `drop type if exists mytype;

create type mytype as (
  a int4,
  b text
);`)
	E(err)
	defer conn.Exec(context.Background(), "drop type mytype")

	qrf := pgx.QueryResultFormats{pgx.BinaryFormatCode}

	var isNull bool
	var a int
	var b *string

	c := pgtype.NewCompositeType(&pgtype.Int4{}, &pgtype.Text{})
	c.Set([]interface{}{2, "bar"})

	err = conn.QueryRow(context.Background(), "select $1::mytype", qrf, c).
		Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("First: isNull=%v a=%d b=%s\n", isNull, a, *b)

	err = conn.QueryRow(context.Background(), "select (1, NULL)::mytype", qrf).Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("Second: isNull=%v a=%d b=%v\n", isNull, a, b)

	err = conn.QueryRow(context.Background(), "select NULL::mytype", qrf).Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("Third: isNull=%v\n", isNull)

	// Output:
	// First: isNull=false a=2 b=bar
	// Second: isNull=false a=1 b=<nil>
	// Third: isNull=true
}
