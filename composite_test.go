package pgtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
)

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

	var isNull bool
	var a int
	var b *string

	c := pgtype.NewComposite(&pgtype.Int4{}, &pgtype.Text{})
	c.SetFields(2, "bar")

	err = conn.QueryRow(context.Background(), "select $1::mytype", c).
		Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("First: isNull=%v a=%d b=%s\n", isNull, a, *b)

	err = conn.QueryRow(context.Background(), "select (1, NULL)::mytype").Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("Second: isNull=%v a=%d b=%v\n", isNull, a, b)

	err = conn.QueryRow(context.Background(), "select NULL::mytype").Scan(c.Scan(&isNull, &a, &b))
	E(err)

	fmt.Printf("Third: isNull=%v\n", isNull)

	// Output:
	// First: isNull=false a=2 b=bar
	// Second: isNull=false a=1 b=<nil>
	// Third: isNull=true
}
