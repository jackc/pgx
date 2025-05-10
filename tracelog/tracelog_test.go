package tracelog_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		return config
	}
}

type testLog struct {
	lvl  tracelog.LogLevel
	msg  string
	data map[string]any
}

type testLogger struct {
	logs []testLog

	mux sync.Mutex
}

func (l *testLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	l.mux.Lock()
	defer l.mux.Unlock()

	data["ctxdata"] = ctx.Value("ctxdata")
	l.logs = append(l.logs, testLog{lvl: level, msg: msg, data: data})
}

func (l *testLogger) Clear() {
	l.mux.Lock()
	defer l.mux.Unlock()

	l.logs = l.logs[0:0]
}

func (l *testLogger) FilterByMsg(msg string) (res []testLog) {
	l.mux.Lock()
	defer l.mux.Unlock()

	for _, log := range l.logs {
		if log.msg == msg {
			res = append(res, log)
		}
	}

	return res
}

func TestContextGetsPassedToLogMethod(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		ctx = context.WithValue(ctx, "ctxdata", "foo")
		_, err := conn.Exec(ctx, `;`)
		require.NoError(t, err)
		require.Len(t, logger.logs, 1)
		require.Equal(t, "foo", logger.logs[0].data["ctxdata"])
	})
}

func TestLoggerFunc(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const testMsg = "foo"

	buf := bytes.Buffer{}
	logger := log.New(&buf, "", 0)

	createAdapterFn := func(logger *log.Logger) tracelog.LoggerFunc {
		return func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			logger.Printf("%s", testMsg)
		}
	}

	config := defaultConnTestRunner.CreateConfig(ctx, t)
	config.Tracer = &tracelog.TraceLog{
		Logger:   createAdapterFn(logger),
		LogLevel: tracelog.LogLevelTrace,
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	buf.Reset() // Clear logs written when establishing connection

	if _, err := conn.Exec(context.TODO(), ";"); err != nil {
		t.Fatal(err)
	}

	if strings.TrimSpace(buf.String()) != testMsg {
		t.Errorf("Expected logger function to return '%s', but it was '%s'", testMsg, buf.String())
	}
}

func TestLogQuery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		_, err := conn.Exec(ctx, `select $1::text`, "testing")
		require.NoError(t, err)

		logs := logger.FilterByMsg("Query")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelInfo, logs[0].lvl)

		logger.Clear()

		_, err = conn.Exec(ctx, `foo`, "testing")
		require.Error(t, err)

		logs = logger.FilterByMsg("Query")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelError, logs[0].lvl)
		require.Equal(t, err, logs[0].data["err"])
	})
}

// https://github.com/jackc/pgx/issues/1365
func TestLogQueryArgsHandlesUTF8(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		var s string
		for i := 0; i < 63; i++ {
			s += "0"
		}
		s += "😊"

		_, err := conn.Exec(ctx, `select $1::text`, s)
		require.NoError(t, err)

		logs := logger.FilterByMsg("Query")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelInfo, logs[0].lvl)
		require.Equal(t, s, logs[0].data["args"].([]any)[0])

		logger.Clear()

		_, err = conn.Exec(ctx, `select $1::text`, s+"000")
		require.NoError(t, err)

		logs = logger.FilterByMsg("Query")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelInfo, logs[0].lvl)
		require.Equal(t, s+" (truncated 3 bytes)", logs[0].data["args"].([]any)[0])
	})
}

func TestLogCopyFrom(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, pgxtest.KnownOIDQueryExecModes, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table foo(a int4)`)
		require.NoError(t, err)

		logger.Clear()

		inputRows := [][]any{
			{int32(1)},
			{nil},
		}

		copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
		require.NoError(t, err)
		require.EqualValues(t, len(inputRows), copyCount)

		logs := logger.FilterByMsg("CopyFrom")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelInfo, logs[0].lvl)

		logger.Clear()

		inputRows = [][]any{
			{"not an integer"},
			{nil},
		}

		copyCount, err = conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
		require.Error(t, err)
		require.EqualValues(t, 0, copyCount)

		logs = logger.FilterByMsg("CopyFrom")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelError, logs[0].lvl)
	})
}

func TestLogConnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	config := defaultConnTestRunner.CreateConfig(ctx, t)
	config.Tracer = tracer

	conn1, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn1.Close(ctx)
	require.Len(t, logger.logs, 1)
	require.Equal(t, "Connect", logger.logs[0].msg)
	require.Equal(t, tracelog.LogLevelInfo, logger.logs[0].lvl)

	logger.Clear()

	config, err = pgx.ParseConfig("host=/invalid")
	require.NoError(t, err)
	config.Tracer = tracer

	conn2, err := pgx.ConnectConfig(ctx, config)
	require.Nil(t, conn2)
	require.Error(t, err)
	require.Len(t, logger.logs, 1)
	require.Equal(t, "Connect", logger.logs[0].msg)
	require.Equal(t, tracelog.LogLevelError, logger.logs[0].lvl)
}

func TestLogBatchStatementsOnExec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		batch := &pgx.Batch{}
		batch.Queue("create table foo (id bigint)")
		batch.Queue("drop table foo")

		br := conn.SendBatch(ctx, batch)

		_, err := br.Exec()
		require.NoError(t, err)

		_, err = br.Exec()
		require.NoError(t, err)

		err = br.Close()
		require.NoError(t, err)

		require.Len(t, logger.logs, 3)
		assert.Equal(t, "BatchQuery", logger.logs[0].msg)
		assert.Equal(t, "create table foo (id bigint)", logger.logs[0].data["sql"])
		assert.Equal(t, "BatchQuery", logger.logs[1].msg)
		assert.Equal(t, "drop table foo", logger.logs[1].data["sql"])
		assert.Equal(t, "BatchClose", logger.logs[2].msg)

	})
}

func TestLogBatchStatementsOnBatchResultClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		batch := &pgx.Batch{}
		batch.Queue("select generate_series(1,$1)", 100)
		batch.Queue("select 1 = 1;")

		br := conn.SendBatch(ctx, batch)
		err := br.Close()
		require.NoError(t, err)

		require.Len(t, logger.logs, 3)
		assert.Equal(t, "BatchQuery", logger.logs[0].msg)
		assert.Equal(t, "select generate_series(1,$1)", logger.logs[0].data["sql"])
		assert.Equal(t, "BatchQuery", logger.logs[1].msg)
		assert.Equal(t, "select 1 = 1;", logger.logs[1].data["sql"])
		assert.Equal(t, "BatchClose", logger.logs[2].msg)
	})
}

func TestLogAcquire(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	config := defaultConnTestRunner.CreateConfig(ctx, t)
	config.Tracer = tracer

	poolConfig, err := pgxpool.ParseConfig(config.ConnString())
	require.NoError(t, err)

	poolConfig.ConnConfig = config
	pool1, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	defer pool1.Close()

	conn1, err := pool1.Acquire(ctx)
	require.NoError(t, err)
	defer conn1.Release()
	require.Len(t, logger.logs, 2) // Has both the Connect and Acquire logs
	require.Equal(t, "Acquire", logger.logs[1].msg)
	require.Equal(t, tracelog.LogLevelDebug, logger.logs[1].lvl)

	logger.Clear()

	// create a 2nd pool with a bad host to verify the error handling
	poolConfig, err = pgxpool.ParseConfig("host=/invalid")
	require.NoError(t, err)
	poolConfig.ConnConfig.Tracer = tracer

	pool2, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	defer pool2.Close()

	conn2, err := pool2.Acquire(ctx)
	require.Error(t, err)
	require.Nil(t, conn2)
	require.Len(t, logger.logs, 2)
	require.Equal(t, "Acquire", logger.logs[1].msg)
	require.Equal(t, tracelog.LogLevelError, logger.logs[1].lvl)
}

func TestLogRelease(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	config := defaultConnTestRunner.CreateConfig(ctx, t)
	config.Tracer = tracer

	poolConfig, err := pgxpool.ParseConfig(config.ConnString())
	require.NoError(t, err)

	poolConfig.ConnConfig = config
	pool1, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	defer pool1.Close()

	conn1, err := pool1.Acquire(ctx)
	require.NoError(t, err)

	logger.Clear()
	conn1.Release()
	require.Len(t, logger.logs, 1)
	require.Equal(t, "Release", logger.logs[0].msg)
	require.Equal(t, tracelog.LogLevelDebug, logger.logs[0].lvl)
}

func TestLogPrepare(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
	}, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		_, err := conn.Exec(ctx, `select $1::text`, "testing")
		require.NoError(t, err)

		logs := logger.FilterByMsg("Prepare")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelInfo, logs[0].lvl)

		logger.Clear()

		_, err = conn.Exec(ctx, `foo aaaa`, "testing")
		require.Error(t, err)

		logs = logger.FilterByMsg("Prepare")
		require.Len(t, logs, 1)
		require.Equal(t, err, logs[0].data["err"])
	})

	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		_, err := conn.Prepare(ctx, "test_query_1", `select $1::int`)
		require.NoError(t, err)

		require.Len(t, logger.logs, 1)
		require.Equal(t, "Prepare", logger.logs[0].msg)
		require.Equal(t, tracelog.LogLevelInfo, logger.logs[0].lvl)

		logger.Clear()

		_, err = conn.Prepare(ctx, `test_query_2`, "foo aaaa")
		require.Error(t, err)

		require.Len(t, logger.logs, 1)
		require.Equal(t, "Prepare", logger.logs[0].msg)
		require.Equal(t, err, logger.logs[0].data["err"])
	})
}

// https://github.com/jackc/pgx/pull/2120
func TestConcurrentUsage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer

	for i := 0; i < 50; i++ {
		func() {
			pool, err := pgxpool.NewWithConfig(ctx, config)
			require.NoError(t, err)

			defer pool.Close()

			eg := errgroup.Group{}

			for i := 0; i < 5; i++ {
				eg.Go(func() error {
					_, err := pool.Exec(ctx, `select 1`)
					return err
				})
			}

			err = eg.Wait()
			require.NoError(t, err)
		}()
	}
}
