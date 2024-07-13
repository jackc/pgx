package multitracer_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type testFullTracer struct{}

func (tt *testFullTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
}

func (tt *testFullTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
}

func (tt *testFullTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
}

func (tt *testFullTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
}

func (tt *testFullTracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
}

func (tt *testFullTracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
}

func (tt *testFullTracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
}

func (tt *testFullTracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
}

type testCopyTracer struct{}

func (tt *testCopyTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
}

func (tt *testCopyTracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
}

func TestNew(t *testing.T) {
	t.Parallel()

	fullTracer := &testFullTracer{}
	copyTracer := &testCopyTracer{}

	mt := multitracer.New(fullTracer, copyTracer)
	require.Equal(
		t,
		&multitracer.Tracer{
			QueryTracers: []pgx.QueryTracer{
				fullTracer,
				copyTracer,
			},
			BatchTracers: []pgx.BatchTracer{
				fullTracer,
			},
			CopyFromTracers: []pgx.CopyFromTracer{
				fullTracer,
				copyTracer,
			},
			PrepareTracers: []pgx.PrepareTracer{
				fullTracer,
			},
			ConnectTracers: []pgx.ConnectTracer{
				fullTracer,
			},
			PoolAcquireTracers: []pgxpool.AcquireTracer{
				fullTracer,
			},
			PoolReleaseTracers: []pgxpool.ReleaseTracer{
				fullTracer,
			},
		},
		mt,
	)
}
