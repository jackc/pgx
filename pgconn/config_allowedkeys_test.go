package pgconn_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseConfigConnStringAllowedKeys(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		connString string
		allowed    []string
		wantErrKey string // empty means parse must succeed
	}{
		{
			name:       "nil slice applies no restriction",
			connString: "host=h user=u dbname=d servicefile=/etc/passwd",
			allowed:    nil,
		},
		{
			name:       "allowed keys pass with dbname alias",
			connString: "user=u dbname=d",
			allowed:    []string{"user", "dbname"},
		},
		{
			name:       "allowed keys pass with canonical database",
			connString: "user=u dbname=d",
			allowed:    []string{"user", "database"},
		},
		{
			name:       "servicefile rejected",
			connString: "user=u dbname=d servicefile=/etc/passwd",
			allowed:    []string{"user", "dbname"},
			wantErrKey: "servicefile",
		},
		{
			name:       "host override rejected",
			connString: "dbname=d host=attacker.example",
			allowed:    []string{"dbname"},
			wantErrKey: "host",
		},
		{
			name:       "sslmode downgrade rejected",
			connString: "dbname=d sslmode=disable",
			allowed:    []string{"dbname"},
			wantErrKey: "sslmode",
		},
		{
			name:       "url form host rejected",
			connString: "postgres://u@attacker.example/d",
			allowed:    []string{"user", "dbname"},
			wantErrKey: "host",
		},
		{
			name:       "empty non-nil slice rejects everything",
			connString: "dbname=d",
			allowed:    []string{},
			wantErrKey: "database",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := pgconn.ParseConfigOptions{ConnStringAllowedKeys: tt.allowed}
			_, err := pgconn.ParseConfigWithOptions(tt.connString, opts)
			if tt.wantErrKey == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error mentioning %q, got nil", tt.wantErrKey)
			}
			if !strings.Contains(err.Error(), tt.wantErrKey) {
				t.Fatalf("error %q does not mention key %q", err, tt.wantErrKey)
			}
		})
	}
}
