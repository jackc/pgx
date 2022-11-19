package tracelog_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}

func (l *testLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	data["ctxdata"] = ctx.Value("ctxdata")
	l.logs = append(l.logs, testLog{lvl: level, msg: msg, data: data})
}

func (l *testLogger) Clear() {
	l.logs = l.logs[0:0]
}

func (l *testLogger) FilterByMsg(msg string) (res []testLog) {
	for _, log := range l.logs {
		if log.msg == msg {
			res = append(res, log)
		}
	}

	return res
}

func TestContextGetsPassedToLogMethod(t *testing.T) {
	t.Parallel()

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		ctx = context.WithValue(context.Background(), "ctxdata", "foo")
		_, err := conn.Exec(ctx, `;`)
		require.NoError(t, err)
		require.Len(t, logger.logs, 1)
		require.Equal(t, "foo", logger.logs[0].data["ctxdata"])
	})
}

func TestLoggerFunc(t *testing.T) {
	t.Parallel()

	const testMsg = "foo"

	buf := bytes.Buffer{}
	logger := log.New(&buf, "", 0)

	createAdapterFn := func(logger *log.Logger) tracelog.LoggerFunc {
		return func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			logger.Printf("%s", testMsg)
		}
	}

	config := defaultConnTestRunner.CreateConfig(context.Background(), t)
	config.Tracer = &tracelog.TraceLog{
		Logger:   createAdapterFn(logger),
		LogLevel: tracelog.LogLevelTrace,
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer conn.Close(context.Background())

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(context.Background(), `create temporary table foo(a int4)`)
		require.NoError(t, err)

		logger.Clear()

		inputRows := [][]any{
			{int32(1)},
			{nil},
		}

		copyCount, err := conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
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

		copyCount, err = conn.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
		require.Error(t, err)
		require.EqualValues(t, 0, copyCount)

		logs = logger.FilterByMsg("CopyFrom")
		require.Len(t, logs, 1)
		require.Equal(t, tracelog.LogLevelError, logs[0].lvl)
	})
}

func TestLogConnect(t *testing.T) {
	t.Parallel()

	logger := &testLogger{}
	tracer := &tracelog.TraceLog{
		Logger:   logger,
		LogLevel: tracelog.LogLevelTrace,
	}

	config := defaultConnTestRunner.CreateConfig(context.Background(), t)
	config.Tracer = tracer

	conn1, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer conn1.Close(context.Background())
	require.Len(t, logger.logs, 1)
	require.Equal(t, "Connect", logger.logs[0].msg)
	require.Equal(t, tracelog.LogLevelInfo, logger.logs[0].lvl)

	logger.Clear()

	config, err = pgx.ParseConfig("host=/invalid")
	require.NoError(t, err)
	config.Tracer = tracer

	conn2, err := pgx.ConnectConfig(context.Background(), config)
	require.Nil(t, conn2)
	require.Error(t, err)
	require.Len(t, logger.logs, 1)
	require.Equal(t, "Connect", logger.logs[0].msg)
	require.Equal(t, tracelog.LogLevelError, logger.logs[0].lvl)
}

func TestLogBatchStatementsOnExec(t *testing.T) {
	t.Parallel()

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		batch := &pgx.Batch{}
		batch.Queue("create table foo (id bigint)")
		batch.Queue("drop table foo")

		br := conn.SendBatch(context.Background(), batch)

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		logger.Clear() // Clear any logs written when establishing connection

		batch := &pgx.Batch{}
		batch.Queue("select generate_series(1,$1)", 100)
		batch.Queue("select 1 = 1;")

		br := conn.SendBatch(context.Background(), batch)
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

func TestLogPrepare(t *testing.T) {
	t.Parallel()

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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, []pgx.QueryExecMode{
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

	pgxtest.RunWithQueryExecModes(context.Background(), t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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
