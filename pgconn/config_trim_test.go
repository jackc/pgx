package pgconn_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseConfigTrimSpace(t *testing.T) {
	// keyword/value form without connecting
	cfg, err := pgconn.ParseConfig("  host=localhost port=5432 dbname=postgres user=u  ")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "localhost" {
		t.Fatalf("host=%q", cfg.Host)
	}
}
