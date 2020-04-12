package pgtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
	errors "golang.org/x/xerrors"
)

type MyType struct {
	a int32   // NULL will cause decoding error
	b *string // there can be NULL in this position in SQL
}

func (dst *MyType) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return errors.New("NULL values can't be decoded. Scan into a &*MyType to handle NULLs")
	}

	a := pgtype.Int4{}
	b := pgtype.Text{}

	if err := pgtype.ScanRowValue(ci, src, &a, &b); err != nil {
		return err
	}

	// type compatibility is checked by AssignTo
	// only lossless assignments will succeed
	if err := a.AssignTo(&dst.a); err != nil {
		return err
	}

	// AssignTo also deals with null value handling
	if err := b.AssignTo(&dst.b); err != nil {
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

	return pgtype.EncodeRow(ci, buf, &a, &b)
}

func ptrS(s string) *string {
	return &s
}

func E(err error) {
	if err != nil {
		panic(err)
	}
}

func Example_compositeTypes() {
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
		pgx.QueryResultFormats{pgx.BinaryFormatCode}, MyType{1, ptrS("foo")}).Scan(&result)
	E(err)

	fmt.Printf("First row: a=%d b=%s\n", result.a, *result.b)

	// Because we scan into &*MyType, NULLs are handled generically by assigning nil to result
	err = conn.QueryRow(context.Background(), "select NULL::mytype", pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(&result)
	E(err)

	fmt.Printf("Second row: %v\n", result)

	// Adhoc rows can be decoded inplace without boilerplate (works with composite types too)
	var isNull bool
	var a int
	var b *string

	err = conn.QueryRow(context.Background(), "select (2, 'bar')::mytype", pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(pgtype.ROW(&isNull, &a, &b))
	E(err)

	fmt.Printf("Adhoc: isNull=%v a=%d b=%s", isNull, a, *b)

	// Output:
	// First row: a=1 b=foo
	// Second row: <nil>
	// Adhoc: isNull=false a=2 b=bar
}
