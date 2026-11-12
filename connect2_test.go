package pgx_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// The shared legacy Cockroach fixture uses an arbitrary startup GUC. Translate
// that intent into the standard options parameter accepted by the new parser.
func newConnectionTestInput(t *testing.T) string {
	t.Helper()
	input := os.Getenv("PGX_TEST_DATABASE")
	if u, err := url.Parse(input); err == nil && (u.Scheme == "postgresql" || u.Scheme == "postgres") {
		q := u.Query()
		if value := q.Get("experimental_enable_temp_tables"); value != "" {
			q.Del("experimental_enable_temp_tables")
			q.Set("options", q.Get("options")+" -c experimental_enable_temp_tables="+value)
			u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
			input = u.String()
		}
	}
	return input
}

func TestConnect2Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	c, err := pgx.Connect2(ctx, input, pgx.ConnectOptions2{})
	require.NoError(t, err)
	defer c.Close(ctx)
	require.Nil(t, c.LegacyConn().Config())
	var n int
	require.NoError(t, c.QueryRow(ctx, "select $1::int", 42).Scan(&n))
	require.Equal(t, 42, n)
	require.NoError(t, c.DeallocateAll(ctx))
	require.NoError(t, c.QueryRow(ctx, "select $1::int", 43).Scan(&n))
	require.Equal(t, 43, n)
	require.NotEmpty(t, c.PgConn().ConnectionInfo().Address)
	_, err = c.Exec(ctx, "create temporary table connect2_copy (n int)")
	require.NoError(t, err)
	copied, err := c.CopyFrom(ctx, pgx.Identifier{"connect2_copy"}, []string{"n"}, pgx.CopyFromRows([][]any{{1}, {2}}))
	require.NoError(t, err)
	require.EqualValues(t, 2, copied)
	batch := &pgx.Batch{}
	batch.Queue("select sum(n)::int from connect2_copy")
	results := c.SendBatch(ctx, batch)
	require.NoError(t, results.QueryRow().Scan(&n))
	require.Equal(t, 3, n)
	require.NoError(t, results.Close())
	tx, err := c.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRow(ctx, "select 4").Scan(&n))
	require.NoError(t, tx.Rollback(ctx))
}

func TestConnectFuncOwnershipAndNotificationHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	calls := 0
	c, err := pgx.ConnectFunc(ctx, func(ctx context.Context) (*pgconn.PgConn, error) {
		calls++
		return pgconn.Connect2(ctx, input, pgconn.ConnectOptions2{AfterConnect: func(ctx context.Context, c *pgconn.PgConn) error {
			if c.ParameterStatus("crdb_version") != "" {
				return nil
			}
			_, err := c.Exec(ctx, "listen handoff; notify handoff, 'from setup'").ReadAll()
			return err
		}})
	}, pgx.ConnOptions{})
	require.NoError(t, err)
	defer c.Close(ctx)
	require.Equal(t, 1, calls)
	if c.PgConn().ParameterStatus("crdb_version") == "" {
		n, err := c.WaitForNotification(ctx)
		require.NoError(t, err)
		require.Equal(t, "from setup", n.Payload)
	}
	sentinel := errors.New("factory failure")
	var low *pgconn.PgConn
	_, err = pgx.ConnectFunc(ctx, func(ctx context.Context) (*pgconn.PgConn, error) {
		var err error
		low, err = pgconn.Connect2(ctx, input, pgconn.ConnectOptions2{})
		if err != nil {
			return nil, err
		}
		return low, sentinel
	}, pgx.ConnOptions{})
	require.ErrorIs(t, err, sentinel)
	require.True(t, low.IsClosed())
	_, err = pgx.ConnectFunc(ctx, func(ctx context.Context) (*pgconn.PgConn, error) {
		var err error
		low, err = pgconn.Connect2(ctx, input, pgconn.ConnectOptions2{})
		return low, err
	}, pgx.ConnOptions{AfterConnect: func(context.Context, *pgx.Conn2) error { return sentinel }})
	require.ErrorIs(t, err, sentinel)
	require.True(t, low.IsClosed())
}

func TestConnect2PoolAndStdlibRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	var maxConns int32 = 1
	var zero int
	opts := pgxpool.Options{MaxConns: &maxConns, Connection: pgx.ConnectOptions2{ConnOptions: pgx.ConnOptions{StatementCacheCapacity: &zero}}}
	t.Setenv("PGAPPNAME", "first")
	pool, err := pgxpool.New2(ctx, input, opts)
	require.NoError(t, err)
	defer pool.Close()
	require.Nil(t, pool.LegacyPool().Config())
	// The captured zero must survive caller mutation. Cache-statement queries
	// fail with the captured zero capacity, whereas Exec still works.
	zero = 512
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Query(ctx, "select 1")
	require.ErrorContains(t, err, "disabled statement cache")
	var app string
	require.NoError(t, conn.QueryRow(ctx, "show application_name", pgx.QueryExecModeSimpleProtocol).Scan(&app))
	require.Equal(t, "first", app)
	pid := conn.Conn().PgConn().PID()
	conn.Release()
	t.Setenv("PGAPPNAME", "second")
	conn, err = pool.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, pid, conn.Conn().PgConn().PID())
	conn.Conn().Close(ctx)
	conn.Release()
	require.NoError(t, pool.QueryRow(ctx, "show application_name", pgx.QueryExecModeSimpleProtocol).Scan(&app))
	require.Equal(t, "second", app)

	db := stdlib.OpenDB2(input, stdlib.Options2{})
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	require.NoError(t, db.QueryRowContext(ctx, "show application_name").Scan(&app))
	require.Equal(t, "second", app)
	t.Setenv("PGAPPNAME", "third")
	require.NoError(t, db.QueryRowContext(ctx, "show application_name").Scan(&app))
	require.Equal(t, "third", app)
	// Pool-backed stdlib retains pool ownership and its selected factory.
	pooledDB := stdlib.OpenDBFromPool2(pool)
	defer pooledDB.Close()
	require.NoError(t, pooledDB.PingContext(ctx))
}

func TestConnect2ConcurrentFactory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	connector := stdlib.GetConnector2(input, stdlib.Options2{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			c, err := connector.Connect(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			c.Close()
		})
	}
	wg.Wait()
}

type (
	factoryTraceKey struct{}
	factoryTracer   struct{ started, ended bool }
)

func (t *factoryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (t *factoryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (t *factoryTracer) TraceConnectStart2(ctx context.Context) context.Context {
	t.started = true
	return context.WithValue(ctx, factoryTraceKey{}, "traced")
}

func (t *factoryTracer) TraceConnectEnd2(_ context.Context, data pgx.TraceConnectEndData2) {
	t.ended = true
}

func TestConnectFuncContextAndValidation(t *testing.T) {
	tracer := &factoryTracer{}
	sentinel := errors.New("test")
	_, err := pgx.ConnectFunc(context.Background(), func(ctx context.Context) (*pgconn.PgConn, error) {
		require.Equal(t, "traced", ctx.Value(factoryTraceKey{}))
		return nil, sentinel
	}, pgx.ConnOptions{Tracer: tracer})
	require.ErrorIs(t, err, sentinel)
	require.True(t, tracer.started)
	require.True(t, tracer.ended)
	_, err = pgx.ConnectFunc(context.Background(), func(context.Context) (*pgconn.PgConn, error) { return nil, nil }, pgx.ConnOptions{})
	require.ErrorContains(t, err, "nil without an error")
	negative := -1
	_, err = pgx.ConnectFunc(context.Background(), func(context.Context) (*pgconn.PgConn, error) {
		t.Fatal("invalid options reached factory")
		return nil, nil
	}, pgx.ConnOptions{StatementCacheCapacity: &negative})
	require.Error(t, err)
}

func TestConnect2RealAuthentication(t *testing.T) {
	for _, key := range []string{"PGX_TEST_MD5_PASSWORD_CONN_STRING", "PGX_TEST_PLAIN_PASSWORD_CONN_STRING", "PGX_TEST_SCRAM_PASSWORD_CONN_STRING", "PGX_TEST_SCRAM_PLUS_CONN_STRING", "PGX_TEST_TLS_CONN_STRING", "PGX_TEST_TLS_CLIENT_CONN_STRING"} {
		t.Run(key, func(t *testing.T) {
			input := os.Getenv(key)
			if input == "" {
				t.Skip("connection string not set")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, err := pgx.Connect2(ctx, input, pgx.ConnectOptions2{ConnectOptions2: pgconn.ConnectOptions2{GetSSLPassword: func(context.Context) string { return os.Getenv("PGX_SSL_PASSWORD") }}})
			require.NoError(t, err)
			defer c.Close(ctx)
			require.NoError(t, c.Ping(ctx))
		})
	}
	input := os.Getenv("PGX_TEST_TLS_CONN_STRING")
	if input == "" {
		return
	}
	t.Run("SNI-independent-verification", func(t *testing.T) {
		ctx := context.Background()
		c, err := pgx.Connect2(ctx, input+" sslsni=0", pgx.ConnectOptions2{})
		require.NoError(t, err)
		defer c.Close(ctx)
		require.True(t, c.PgConn().ConnectionInfo().TLS)
	})
	t.Run("direct-TLS", func(t *testing.T) {
		ctx := context.Background()
		check, err := pgx.Connect2(ctx, input, pgx.ConnectOptions2{})
		require.NoError(t, err)
		var version int
		err = check.QueryRow(ctx, "select current_setting('server_version_num')::int").Scan(&version)
		check.Close(ctx)
		require.NoError(t, err)
		if version < 170000 {
			t.Skip("direct TLS requires PostgreSQL 17+")
		}
		c, err := pgx.Connect2(ctx, input+" sslnegotiation=direct", pgx.ConnectOptions2{})
		require.NoError(t, err)
		defer c.Close(ctx)
		require.True(t, c.PgConn().ConnectionInfo().TLS)
	})
}

func TestConnect2PoolHookOrderAndAdmissionCleanup(t *testing.T) {
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var order []string
	var rejected *pgx.Conn
	sentinel := errors.New("reject admission")
	pool, err := pgxpool.New2(ctx, input, pgxpool.Options{
		Connection: pgx.ConnectOptions2{
			ConnectOptions2: pgconn.ConnectOptions2{AfterConnect: func(context.Context, *pgconn.PgConn) error { order = append(order, "pgconn"); return nil }},
			ConnOptions:     pgx.ConnOptions{AfterConnect: func(context.Context, *pgx.Conn2) error { order = append(order, "pgx"); return nil }},
		},
		AfterConnect: func(_ context.Context, c *pgx.Conn) error {
			order = append(order, "pool")
			rejected = c
			return sentinel
		},
	})
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Acquire(ctx)
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, []string{"pgconn", "pgx", "pool"}, order)
	require.True(t, rejected.IsClosed())
	require.Zero(t, pool.Stat().TotalConns())
}

func TestConnect2LegacyStdlibAdmissionCleanup(t *testing.T) {
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	config, err := pgx.ParseConfig(input)
	require.NoError(t, err)
	sentinel := errors.New("legacy admission rejected")
	var rejected *pgx.Conn
	connector := stdlib.GetConnector(*config, stdlib.OptionAfterConnect(func(_ context.Context, c *pgx.Conn) error { rejected = c; return sentinel }))
	_, err = connector.Connect(context.Background())
	require.ErrorIs(t, err, sentinel)
	require.True(t, rejected.IsClosed())
}
