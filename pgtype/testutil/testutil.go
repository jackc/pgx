package testutil

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func MustConnectPgx(t testing.TB) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func MustClose(t testing.TB, conn interface {
	Close() error
}) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func MustCloseContext(t testing.TB, conn interface {
	Close(context.Context) error
}) {
	err := conn.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

type TranscodeTestCase struct {
	Src  interface{}
	Dst  interface{}
	Test func(interface{}) bool
}

func RunTranscodeTests(t testing.TB, pgTypeName string, tests []TranscodeTestCase) {
	conn := MustConnectPgx(t)
	defer MustCloseContext(t, conn)

	formats := []struct {
		name string
		code int16
	}{
		{name: "TextFormat", code: pgx.TextFormatCode},
		{name: "BinaryFormat", code: pgx.BinaryFormatCode},
	}

	for _, format := range formats {
		RunTranscodeTestsFormat(t, pgTypeName, tests, conn, format.name, format.code)
	}
}

func RunTranscodeTestsFormat(t testing.TB, pgTypeName string, tests []TranscodeTestCase, conn *pgx.Conn, formatName string, formatCode int16) {
	_, err := conn.Prepare(context.Background(), "test", fmt.Sprintf("select $1::%s", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	for i, tt := range tests {
		err := conn.QueryRow(context.Background(), "test", pgx.QueryResultFormats{formatCode}, tt.Src).Scan(tt.Dst)
		if err != nil {
			t.Errorf("%s %d: %v", formatName, i, err)
		}

		dst := reflect.ValueOf(tt.Dst)
		if dst.Kind() == reflect.Ptr {
			dst = dst.Elem()
		}

		if !tt.Test(dst.Interface()) {
			t.Errorf("%s %d: unexpected result for %v: %v", formatName, i, tt.Src, dst.Interface())
		}
	}
}
