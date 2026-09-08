package pgx_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/internal/faultyconn"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

type testTracer struct {
	traceQueryStart    func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context
	traceQueryEnd      func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData)
	traceBatchStart    func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context
	traceBatchQuery    func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData)
	traceBatchEnd      func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData)
	traceCopyFromStart func(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context
	traceCopyFromEnd   func(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData)
	tracePrepareStart  func(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context
	tracePrepareEnd    func(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData)
	traceConnectStart  func(ctx context.Context, data pgx.TraceConnectStartData) context.Context
	traceConnectEnd    func(ctx context.Context, data pgx.TraceConnectEndData)
}

type ctxKey string

func (tt *testTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if tt.traceQueryStart != nil {
		return tt.traceQueryStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if tt.traceQueryEnd != nil {
		tt.traceQueryEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if tt.traceBatchStart != nil {
		return tt.traceBatchStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	if tt.traceBatchQuery != nil {
		tt.traceBatchQuery(ctx, conn, data)
	}
}

func (tt *testTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	if tt.traceBatchEnd != nil {
		tt.traceBatchEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	if tt.traceCopyFromStart != nil {
		return tt.traceCopyFromStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	if tt.traceCopyFromEnd != nil {
		tt.traceCopyFromEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if tt.tracePrepareStart != nil {
		return tt.tracePrepareStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if tt.tracePrepareEnd != nil {
		tt.tracePrepareEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if tt.traceConnectStart != nil {
		return tt.traceConnectStart(ctx, data)
	}
	return ctx
}

func (tt *testTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if tt.traceConnectEnd != nil {
		tt.traceConnectEnd(ctx, data)
	}
}

func TestTraceExec(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceQueryStartCalled := false
		tracer.traceQueryStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
			traceQueryStartCalled = true
			require.Equal(t, `select $1::text`, data.SQL)
			require.Len(t, data.Args, 1)
			require.Equal(t, `testing`, data.Args[0])
			return context.WithValue(ctx, ctxKey("fromTraceQueryStart"), "foo")
		}

		traceQueryEndCalled := false
		tracer.traceQueryEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
			traceQueryEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceQueryStart")))
			require.Equal(t, `SELECT 1`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		_, err := conn.Exec(ctx, `select $1::text`, "testing")
		require.NoError(t, err)
		require.True(t, traceQueryStartCalled)
		require.True(t, traceQueryEndCalled)
	})
}

func TestTraceQuery(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceQueryStartCalled := false
		tracer.traceQueryStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
			traceQueryStartCalled = true
			require.Equal(t, `select $1::text`, data.SQL)
			require.Len(t, data.Args, 1)
			require.Equal(t, `testing`, data.Args[0])
			return context.WithValue(ctx, ctxKey("fromTraceQueryStart"), "foo")
		}

		traceQueryEndCalled := false
		tracer.traceQueryEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
			traceQueryEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceQueryStart")))
			require.Equal(t, `SELECT 1`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		var s string
		err := conn.QueryRow(ctx, `select $1::text`, "testing").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "testing", s)
		require.True(t, traceQueryStartCalled)
		require.True(t, traceQueryEndCalled)
	})
}

func TestTraceBatchNormal(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 2, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)

		var n int32
		err := br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		require.EqualValues(t, 1, traceBatchQueryCalledCount)

		err = br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)

		err = br.Close()
		require.NoError(t, err)

		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchClose(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 2, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)
		err := br.Close()
		require.NoError(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchErrorWhileReadingResults(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol}, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 3, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			if traceBatchQueryCalledCount == 2 {
				require.Error(t, data.Err)
			} else {
				require.NoError(t, data.Err)
			}
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.Error(t, data.Err)
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2/n-2 from generate_series(0,10) n`)
		batch.Queue(`select 3`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)

		commandTag, err := br.Exec()
		require.NoError(t, err)
		require.Equal(t, "SELECT 1", commandTag.String())

		commandTag, err = br.Exec()
		require.Error(t, err)
		require.Equal(t, "", commandTag.String())

		commandTag, err = br.Exec()
		require.Error(t, err)
		require.Equal(t, "", commandTag.String())

		err = br.Close()
		require.Error(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchErrorWhileReadingResultsWhileClosing(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol}, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 3, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			if traceBatchQueryCalledCount == 2 {
				require.Error(t, data.Err)
			} else {
				require.NoError(t, data.Err)
			}
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.Error(t, data.Err)
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2/n-2 from generate_series(0,10) n`)
		batch.Queue(`select 3`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)
		err := br.Close()
		require.Error(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

// TestTraceBatchQueryReadStartTime asserts that TraceBatchQueryData.ReadStartTime is populated for every execution
// mode and consumption path (Exec, QueryRow, and Query followed by a deferred Rows.Close), and that it advances
// monotonically across a batch. It also asserts that, for Query, TraceBatchQuery is not emitted until the returned
// Rows is closed -- a pause while the application holds Rows open must not be excluded from the traced interval.
func TestTraceBatchQueryReadStartTime(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		beforeSendBatch := time.Now()

		var readStartTimes []time.Time
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			require.NoError(t, data.Err)
			require.Falsef(t, data.ReadStartTime.IsZero(), "ReadStartTime must be populated")
			require.Falsef(t, data.ReadStartTime.Before(beforeSendBatch), "ReadStartTime must not predate SendBatch")
			readStartTimes = append(readStartTimes, data.ReadStartTime)
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2`)
		batch.Queue(`select 3`)

		br := conn.SendBatch(context.Background(), batch)

		// Exec consumes the first query immediately; TraceBatchQuery fires before Exec returns.
		_, err := br.Exec()
		require.NoError(t, err)
		require.Len(t, readStartTimes, 1)

		time.Sleep(5 * time.Millisecond)

		// QueryRow consumes the second query immediately too.
		var n int32
		err = br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)
		require.Len(t, readStartTimes, 2)

		time.Sleep(5 * time.Millisecond)

		// Query defers TraceBatchQuery until the returned Rows is closed: reading the row and pausing before Close
		// must not produce a trace yet, even though pgx already began reading the third query's result.
		rows, err := br.Query()
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&n))
		require.EqualValues(t, 3, n)
		// Deliberately do not drain to exhaustion (which would auto-close Rows); leave Rows open and pause, as an
		// application processing a row would, then close explicitly.
		require.Len(t, readStartTimes, 2, "TraceBatchQuery must not fire until Rows is closed")

		time.Sleep(5 * time.Millisecond)
		rows.Close()
		require.NoError(t, rows.Err())
		require.Len(t, readStartTimes, 3)

		err = br.Close()
		require.NoError(t, err)
		afterClose := time.Now()

		require.Len(t, readStartTimes, 3)
		for _, readStartTime := range readStartTimes {
			require.Falsef(t, readStartTime.After(afterClose), "ReadStartTime must not postdate the batch close")
		}
		for i := 1; i < len(readStartTimes); i++ {
			require.Falsef(t, readStartTimes[i].Before(readStartTimes[i-1]),
				"ReadStartTime must be monotonic across a batch: query %d (%v) is before query %d (%v)",
				i, readStartTimes[i], i-1, readStartTimes[i-1])
		}
	})
}

// TestTraceBatchQueryTracesPipelineGetResultsError asserts that pipelineBatchResults.Exec emits TraceBatchQuery even
// when the underlying pipeline.GetResults call itself fails -- e.g. because the context was canceled -- rather than
// only when the query completes and returns a normal query error. Before this fix, that path returned silently
// without ever calling TraceBatchQuery, leaving the query untraced.
func TestTraceBatchQueryTracesPipelineGetResultsError(t *testing.T) {
	// Not parallel because it deterministically kills the underlying connection.

	tracer := &testTracer{}

	var faultyConn *faultyconn.Conn
	ctr := pgxtest.DefaultConnTestRunner()
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		config.Tracer = tracer
		config.AfterNetConnect = func(ctx context.Context, config *pgconn.Config, conn net.Conn) (net.Conn, error) {
			faultyConn = faultyconn.New(conn)
			return faultyConn, nil
		}
		return config
	}
	// The test kills the connection deliberately, so a clean Close is not expected.
	ctr.CloseConn = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_ = conn.Close(ctx)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Only extended-protocol modes route through pipelineBatchResults; QueryExecModeExec and
	// QueryExecModeSimpleProtocol use batchResults instead, which already traced this error before this fix.
	pipelineModes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
	}

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, pipelineModes, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		traceBatchQueryCalledCount := 0
		var lastErr error
		tracer.traceBatchQuery = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			lastErr = data.Err
		}

		batch := &pgx.Batch{}
		batch.Queue(`select 1`)

		br := conn.SendBatch(context.Background(), batch)

		// Kill the connection before GetResults is called, so it fails at the client level (a read/protocol error),
		// not with a normal PgError -- deterministically reproducing the path pipelineBatchResults.Exec used to
		// return from without ever calling TraceBatchQuery.
		require.NoError(t, faultyConn.Close())

		_, err := br.Exec()
		require.Error(t, err)
		require.EqualValues(t, 1, traceBatchQueryCalledCount, "TraceBatchQuery must fire even when GetResults itself fails")
		require.Error(t, lastErr)

		br.Close()
	})
}

func TestTraceCopyFrom(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		traceCopyFromStartCalled := false
		tracer.traceCopyFromStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
			traceCopyFromStartCalled = true
			require.Equal(t, pgx.Identifier{"foo"}, data.TableName)
			require.Equal(t, []string{"a"}, data.ColumnNames)
			return context.WithValue(ctx, ctxKey("fromTraceCopyFromStart"), "foo")
		}

		traceCopyFromEndCalled := false
		tracer.traceCopyFromEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
			traceCopyFromEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceCopyFromStart")))
			require.Equal(t, `COPY 2`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		_, err := conn.Exec(ctx, `create temporary table foo(a int4)`)
		require.NoError(t, err)

		inputRows := [][]any{
			{int32(1)},
			{nil},
		}

		copyCount, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"a"}, pgx.CopyFromRows(inputRows))
		require.NoError(t, err)
		require.EqualValues(t, len(inputRows), copyCount)
		require.True(t, traceCopyFromStartCalled)
		require.True(t, traceCopyFromEndCalled)
	})
}

func TestTracePrepare(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		tracePrepareStartCalled := false
		tracer.tracePrepareStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
			tracePrepareStartCalled = true
			require.Equal(t, `ps`, data.Name)
			require.Equal(t, `select $1::text`, data.SQL)
			return context.WithValue(ctx, ctxKey("fromTracePrepareStart"), "foo")
		}

		tracePrepareEndCalled := false
		tracer.tracePrepareEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
			tracePrepareEndCalled = true
			require.False(t, data.AlreadyPrepared)
			require.NoError(t, data.Err)
		}

		_, err := conn.Prepare(ctx, "ps", `select $1::text`)
		require.NoError(t, err)
		require.True(t, tracePrepareStartCalled)
		require.True(t, tracePrepareEndCalled)

		tracePrepareStartCalled = false
		tracePrepareEndCalled = false
		tracer.tracePrepareEnd = func(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
			tracePrepareEndCalled = true
			require.True(t, data.AlreadyPrepared)
			require.NoError(t, data.Err)
		}

		_, err = conn.Prepare(ctx, "ps", `select $1::text`)
		require.NoError(t, err)
		require.True(t, tracePrepareStartCalled)
		require.True(t, tracePrepareEndCalled)
	})
}

func TestTraceConnect(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	config := defaultConnTestRunner.CreateConfig(context.Background(), t)
	config.Tracer = tracer

	traceConnectStartCalled := false
	tracer.traceConnectStart = func(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
		traceConnectStartCalled = true
		require.NotNil(t, data.ConnConfig)
		return context.WithValue(ctx, ctxKey("fromTraceConnectStart"), "foo")
	}

	traceConnectEndCalled := false
	tracer.traceConnectEnd = func(ctx context.Context, data pgx.TraceConnectEndData) {
		traceConnectEndCalled = true
		require.NotNil(t, data.Conn)
		require.NoError(t, data.Err)
	}

	conn1, err := pgx.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer conn1.Close(context.Background())
	require.True(t, traceConnectStartCalled)
	require.True(t, traceConnectEndCalled)

	config, err = pgx.ParseConfig("host=/invalid")
	require.NoError(t, err)
	config.Tracer = tracer

	traceConnectStartCalled = false
	traceConnectEndCalled = false
	tracer.traceConnectEnd = func(ctx context.Context, data pgx.TraceConnectEndData) {
		traceConnectEndCalled = true
		require.Nil(t, data.Conn)
		require.Error(t, data.Err)
	}

	conn2, err := pgx.ConnectConfig(context.Background(), config)
	require.Nil(t, conn2)
	require.Error(t, err)
	require.True(t, traceConnectStartCalled)
	require.True(t, traceConnectEndCalled)
}

// Ensure tracer runs within a transaction.
//
// https://github.com/jackc/pgx/issues/2304
func TestTraceWithinTx(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var queries []string
		tracer.traceQueryStart = func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
			queries = append(queries, data.SQL)
			return ctx
		}

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `select $1::text`, "testing")
		require.NoError(t, err)
		err = tx.Commit(ctx)
		require.NoError(t, err)

		require.Len(t, queries, 3)
		require.Equal(t, `begin`, queries[0])
		require.Equal(t, `select $1::text`, queries[1])
		require.Equal(t, `commit`, queries[2])
	})
}
