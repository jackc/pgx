package pgx_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
)

type timeoutErrTest struct {
	executor             string // Whether to use Exec() or Query()
	preferSimpleProtocol bool   // Disables implicit prepared statement usage
	timeoutMethod        string // Whether a timeout is triggered by canceling a context or by passing its deadline
}

func TestTimeoutErr(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))

	// Generate all permutations of timeoutErrTest.
	tests := make([]timeoutErrTest, 8)
	for i, executor := range []string{"exec", "query"} {
		for j, preferSimpleProtocol := range []bool{true, false} {
			for k, timeoutMethod := range []string{"cancel", "deadline"} {
				idx := (i * 4) + (j * 2) + k
				tests[idx] = timeoutErrTest{executor, preferSimpleProtocol, timeoutMethod}
			}
		}
	}

	for _, tt := range tests {
		func() {
			config.PreferSimpleProtocol = tt.preferSimpleProtocol
			conn := mustConnect(t, config)
			defer closeConn(t, conn)

			var ctx context.Context
			var cancel context.CancelFunc
			switch tt.timeoutMethod {
			case "cancel":
				ctx, cancel = context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Millisecond * 100)
					cancel()
				}()
			case "deadline":
				d := time.Now().Add(time.Millisecond * 100)
				ctx, cancel = context.WithDeadline(context.Background(), d)
				defer cancel()
			default:
				t.Fatalf("unexpected timeout method: %v", tt.timeoutMethod)
			}

			var err error
			switch tt.executor {
			case "exec":
				_, err = conn.Exec(ctx, "select pg_sleep(5);")
			case "query":
				var rows pgx.Rows
				rows, err = conn.Query(ctx, "select pg_sleep(5);")
				// When querying with the extended protocol, the error only appears after reading the first row.
				if !tt.preferSimpleProtocol {
					rows.Next()
					err = rows.Err()
				}
			default:
				t.Fatalf("unexpected executor: %v", tt.executor)
			}

			var e *pgx.ErrPostgresTimeout
			if !errors.As(err, &e) {
				t.Errorf("expected ErrPostgresTimeout, received %v - %+v", err, tt)
			}
		}()
	}
}
