package pgx_test

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/stretchr/testify/require"
)

func TestConnect2ConfigSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	input := newConnectionTestInput(t)
	if input == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}
	capacity, description := 0, 7
	mode := pgx.QueryExecModeExec
	tracer := &connect2Tracer{onStart: func(config *pgx.ConnConfig) {
		require.Equal(t, input, config.ConnString())
		*config.Connect2Options.StatementCacheCapacity = 99
		config.Connect2String = "invalid"
	}}
	opts := pgx.ConnectOptions2{ConnOptions: pgx.ConnOptions{
		StatementCacheCapacity: &capacity, DescriptionCacheCapacity: &description,
		DefaultQueryExecMode: &mode, Tracer: multitracer.New(tracer),
		AfterConnect: func(_ context.Context, c *pgx.Conn) error {
			require.Equal(t, input, c.Config().ConnString())
			require.Equal(t, 0, c.Config().StatementCacheCapacity)
			return nil
		},
	}}
	t.Setenv("PGAPPNAME", "snapshot-first")
	c, err := pgx.Connect2(ctx, input, opts)
	require.NoError(t, err)
	defer c.Close(ctx)
	capacity, description, mode = 128, 256, pgx.QueryExecModeSimpleProtocol
	config := c.Config()
	require.Equal(t, input, config.ConnString())
	require.Equal(t, pgconn.Config{}, config.Config)
	require.Equal(t, 0, config.StatementCacheCapacity)
	require.Equal(t, 7, config.DescriptionCacheCapacity)
	require.Equal(t, pgx.QueryExecModeExec, config.DefaultQueryExecMode)
	require.Equal(t, 0, *config.Connect2Options.StatementCacheCapacity)
	require.Equal(t, 7, *config.Connect2Options.DescriptionCacheCapacity)
	require.Equal(t, pgx.QueryExecModeExec, *config.Connect2Options.DefaultQueryExecMode)
	copy := config.Copy()
	*copy.Connect2Options.StatementCacheCapacity = 3
	*copy.Connect2Options.DescriptionCacheCapacity = 4
	*copy.Connect2Options.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	require.Equal(t, 0, *config.Connect2Options.StatementCacheCapacity)
	require.Equal(t, 7, *config.Connect2Options.DescriptionCacheCapacity)
	require.Equal(t, pgx.QueryExecModeExec, *config.Connect2Options.DefaultQueryExecMode)
	*config.Connect2Options.StatementCacheCapacity = 5
	require.Equal(t, 0, *c.Config().Connect2Options.StatementCacheCapacity)

	config = c.Config()
	t.Setenv("PGAPPNAME", "snapshot-second")
	reconnected, err := pgx.Connect2(ctx, config.ConnString(), *config.Connect2Options)
	require.NoError(t, err)
	defer reconnected.Close(ctx)
	var app string
	require.NoError(t, reconnected.QueryRow(ctx, "show application_name").Scan(&app))
	require.Equal(t, "snapshot-second", app)
	require.NoError(t, c.QueryRow(ctx, "show application_name").Scan(&app))
	require.Equal(t, "snapshot-first", app)

	// The private origin guards survive copying and clearing every new public field.
	config = config.Copy()
	config.Connect2Options, config.Connect2String = nil, ""
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = pgx.ConnectConfig(ctx, config) })
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = pgconn.ConnectConfig(ctx, config.Config.Copy()) })
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = stdlib.GetConnector(*config).Connect(ctx) })

	legacy, err := pgx.ParseConfig(input)
	require.NoError(t, err)
	legacy.Connect2String = "invalid"
	legacy.Connect2Options = &pgx.ConnectOptions2{ConnOptions: pgx.ConnOptions{AfterConnect: func(context.Context, *pgx.Conn) error {
		t.Fatal("legacy connection used Connect2 options")
		return nil
	}}}
	old, err := pgx.ConnectConfig(ctx, legacy)
	require.NoError(t, err)
	defer old.Close(ctx)
	require.Equal(t, input, old.Config().ConnString())
}

func TestNew2ConfigSnapshot(t *testing.T) {
	ctx := context.Background()
	maxConns := int32(1)
	capacity := 0
	lifetime, idle, health, timeout := time.Duration(0), time.Hour, time.Minute, time.Duration(0)
	keys := []string{"host"}
	opts := pgxpool.Options{
		MaxConns: maxConns, MaxConnLifetime: &lifetime, MaxConnIdleTime: &idle,
		HealthCheckPeriod: health, ConnectTimeout: &timeout,
		Connection: pgx.ConnectOptions2{
			ConnectOptions2: pgconn.ConnectOptions2{ConnStringAllowedKeys: keys},
			ConnOptions:     pgx.ConnOptions{StatementCacheCapacity: &capacity},
		},
	}
	pool, err := pgxpool.New2(ctx, "host=example.invalid", opts)
	require.NoError(t, err)
	defer pool.Close()
	capacity = 9
	lifetime, idle, timeout = time.Second, time.Second, time.Second
	keys[0] = "invalid"
	config := pool.Config()
	require.Equal(t, "host=example.invalid", config.ConnString())
	require.EqualValues(t, 1, config.MaxConns)
	require.Zero(t, config.MaxConnLifetime)
	require.Equal(t, time.Hour, config.MaxConnIdleTime)
	require.Equal(t, time.Minute, config.HealthCheckPeriod)
	require.EqualValues(t, 1, config.New2Options.MaxConns)
	require.Zero(t, *config.New2Options.MaxConnLifetime)
	require.Equal(t, time.Hour, *config.New2Options.MaxConnIdleTime)
	require.Equal(t, time.Minute, config.New2Options.HealthCheckPeriod)
	require.Zero(t, *config.New2Options.ConnectTimeout)
	require.Zero(t, *config.New2Options.Connection.StatementCacheCapacity)
	require.Nil(t, config.New2Options.Connection.DescriptionCacheCapacity)
	require.Equal(t, []string{"host"}, config.New2Options.Connection.ConnStringAllowedKeys)
	copy := config.Copy()
	copy.New2Options.MaxConns = 2
	*copy.New2Options.MaxConnLifetime = time.Second
	*copy.New2Options.MaxConnIdleTime = time.Second
	copy.New2Options.HealthCheckPeriod = time.Second
	*copy.New2Options.ConnectTimeout = time.Second
	*copy.New2Options.Connection.StatementCacheCapacity = 5
	copy.New2Options.Connection.ConnStringAllowedKeys[0] = "invalid"
	copy.ConnConfig.Connect2Options.ConnStringAllowedKeys[0] = "invalid"
	require.Equal(t, pool.Config(), config)
	copy = config.Copy()
	copy.New2Options = nil
	copy.ConnConfig.Connect2String, copy.ConnConfig.Connect2Options = "", nil
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = pgxpool.NewWithConfig(ctx, copy) })
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = pgx.ConnectConfig(ctx, copy.ConnConfig) })
	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { _, _ = pgconn.ConnectConfig(ctx, copy.ConnConfig.Config.Copy()) })

	recreated, err := pgxpool.New2(ctx, config.ConnString(), *config.New2Options)
	require.NoError(t, err)
	defer recreated.Close()
	require.Equal(t, config, recreated.Config())
}

func TestConnect2TraceSnapshotAndLogging(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("dial failed")
	keys := []string{"host", "dbname", "password", "sslmode"}
	input := "host=127.0.0.1 dbname=trace_test password=do-not-log sslmode=disable"
	tracer := &connect2Tracer{onStart: func(config *pgx.ConnConfig) {
		config.Connect2String = "invalid"
		config.Connect2Options.ConnStringAllowedKeys[0] = "invalid"
		config.Connect2Options.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("trace snapshot changed the dialer")
			return nil, nil
		}
	}}
	var logs []map[string]any
	logger := &tracelog.TraceLog{LogLevel: tracelog.LogLevelInfo, Logger: tracelog.LoggerFunc(func(_ context.Context, _ tracelog.LogLevel, _ string, fields map[string]any) {
		logs = append(logs, fields)
	})}
	_, err := pgx.Connect2(ctx, input, pgx.ConnectOptions2{
		ConnectOptions2: pgconn.ConnectOptions2{ConnStringAllowedKeys: keys, DialFunc: func(ctx context.Context, _, _ string) (net.Conn, error) {
			require.Equal(t, "traced", ctx.Value(connect2TraceKey{}))
			return nil, sentinel
		}},
		ConnOptions: pgx.ConnOptions{Tracer: multitracer.New(logger, tracer)},
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, input, tracer.config.ConnString())
	require.Equal(t, []string{"host", "dbname", "password", "sslmode"}, keys)
	require.Len(t, logs, 1)
	require.Equal(t, "127.0.0.1", logs[0]["host"])
	require.Equal(t, "trace_test", logs[0]["database"])
	require.NotContains(t, logs[0], "password")
	require.NotContains(t, logs[0], "connString")
}

func TestNew2ScalarDefaults(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		opts     pgxpool.Options
		maxConns int32
		period   time.Duration
	}{
		{name: "defaults", maxConns: max(4, int32(runtime.NumCPU())), period: time.Minute},
		{name: "explicit", opts: pgxpool.Options{MaxConns: 2, HealthCheckPeriod: 5 * time.Minute}, maxConns: 2, period: 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := pgxpool.New2(ctx, "", tc.opts)
			require.NoError(t, err)
			defer pool.Close()
			config := pool.Config()
			require.Equal(t, tc.maxConns, config.MaxConns)
			require.Equal(t, tc.period, config.HealthCheckPeriod)
			require.Equal(t, tc.opts.MaxConns, config.New2Options.MaxConns)
			require.Equal(t, tc.opts.HealthCheckPeriod, config.New2Options.HealthCheckPeriod)
		})
	}
	for _, opts := range []pgxpool.Options{{MaxConns: -1}, {HealthCheckPeriod: -time.Second}} {
		pool, err := pgxpool.New2(ctx, "", opts)
		require.Error(t, err)
		require.Nil(t, pool)
	}
}
