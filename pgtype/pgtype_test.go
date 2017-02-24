package pgtype_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx"
)

func mustConnectPgx(t testing.TB) *pgx.Conn {
	config, err := pgx.ParseURI(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func mustClose(t testing.TB, conn interface {
	Close() error
}) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}
