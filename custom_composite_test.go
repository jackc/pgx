package pgtype_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
)

type MyType struct {
	a int32   // NULL will cause decoding error
	b *string // there can be NULL in this position in SQL
}

func (dst *MyType) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return errors.New("NULL values can't be decoded. Scan into a &*MyType to handle NULLs")
	}

	if err := (pgtype.CompositeFields{&dst.a, &dst.b}).DecodeBinary(ci, src); err != nil {
		return err
	}

	return nil
}

func (src MyType) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	a := pgtype.Int4{src.a, pgtype.Present}
	var b pgtype.Text
	if src.b != nil {
		b = pgtype.Text{*src.b, pgtype.Present}
	} else {
		b = pgtype.Text{Status: pgtype.Null}
	}

	return (pgtype.CompositeFields{&a, &b}).EncodeBinary(ci, buf)
}

func ptrS(s string) *string {
	return &s
}

func E(err error) {
	if err != nil {
		panic(err)
	}
}

// ExampleCustomCompositeTypes demonstrates how support for custom types mappable to SQL
// composites can be added.
func Example_customCompositeTypes() {
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

	var result *MyType

	// Demonstrates both passing and reading back composite values
	err = conn.QueryRow(context.Background(), "select $1::mytype",
		pgx.QueryResultFormats{pgx.BinaryFormatCode}, MyType{1, ptrS("foo")}).
		Scan(&result)
	E(err)

	fmt.Printf("First row: a=%d b=%s\n", result.a, *result.b)

	// Because we scan into &*MyType, NULLs are handled generically by assigning nil to result
	err = conn.QueryRow(context.Background(), "select NULL::mytype", pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(&result)
	E(err)

	fmt.Printf("Second row: %v\n", result)

	// Output:
	// First row: a=1 b=foo
	// Second row: <nil>
}
