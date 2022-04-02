// Package pgxtest provides utilities for testing pgx and packages that integrate with pgx.
package pgxtest

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// ConnTestRunner controls how a *pgx.Conn is created and closed by tests. All fields are required. Use DefaultConnTestRunner to get a
// ConnTestRunner with reasonable default values.
type ConnTestRunner struct {
	// CreateConfig returns a *pgx.ConnConfig suitable for use with pgx.ConnectConfig.
	CreateConfig func(ctx context.Context, t testing.TB) *pgx.ConnConfig

	// AfterConnect is called after conn is established. It allows for arbitrary connection setup before a test begins.
	AfterConnect func(ctx context.Context, t testing.TB, conn *pgx.Conn)

	// AfterTest is called after the test is run. It allows for validating the state of the connection before it is closed.
	AfterTest func(ctx context.Context, t testing.TB, conn *pgx.Conn)

	// CloseConn closes conn.
	CloseConn func(ctx context.Context, t testing.TB, conn *pgx.Conn)
}

// DefaultConnTestRunner returns a new ConnTestRunner with all fields set to reasonable default values.
func DefaultConnTestRunner() ConnTestRunner {
	return ConnTestRunner{
		CreateConfig: func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
			config, err := pgx.ParseConfig("")
			if err != nil {
				t.Fatalf("ParseConfig failed: %v", err)
			}
			return config
		},
		AfterConnect: func(ctx context.Context, t testing.TB, conn *pgx.Conn) {},
		AfterTest:    func(ctx context.Context, t testing.TB, conn *pgx.Conn) {},
		CloseConn: func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
			err := conn.Close(ctx)
			if err != nil {
				t.Errorf("Close failed: %v", err)
			}
		},
	}
}

func (ctr *ConnTestRunner) RunTest(ctx context.Context, t testing.TB, f func(ctx context.Context, t testing.TB, conn *pgx.Conn)) {
	config := ctr.CreateConfig(ctx, t)
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("ConnectConfig failed: %v", err)
	}
	defer ctr.CloseConn(ctx, t, conn)

	ctr.AfterConnect(ctx, t, conn)
	f(ctx, t, conn)
	ctr.AfterTest(ctx, t, conn)
}

// RunWithQueryExecModes runs a f in a new test for each element of modes with a new connection created using connector.
// If modes is nil all pgx.QueryExecModes are tested.
func RunWithQueryExecModes(ctx context.Context, t *testing.T, ctr ConnTestRunner, modes []pgx.QueryExecMode, f func(ctx context.Context, t testing.TB, conn *pgx.Conn)) {
	if modes == nil {
		modes = []pgx.QueryExecMode{
			pgx.QueryExecModeCacheStatement,
			pgx.QueryExecModeCacheDescribe,
			pgx.QueryExecModeDescribeExec,
			pgx.QueryExecModeExec,
			pgx.QueryExecModeSimpleProtocol,
		}
	}

	for _, mode := range modes {
		ctrWithMode := ctr
		ctrWithMode.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
			config := ctr.CreateConfig(ctx, t)
			config.DefaultQueryExecMode = mode
			return config
		}

		t.Run(mode.String(),
			func(t *testing.T) {
				ctrWithMode.RunTest(ctx, t, f)
			},
		)
	}
}
