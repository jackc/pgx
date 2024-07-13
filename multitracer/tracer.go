// Package multitracer provides a Tracer that can combine several tracers into one.
package multitracer

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tracer can combine several tracers into one.
// You can use New to automatically split tracers by interface.
type Tracer struct {
	QueryTracers       []pgx.QueryTracer
	BatchTracers       []pgx.BatchTracer
	CopyFromTracers    []pgx.CopyFromTracer
	PrepareTracers     []pgx.PrepareTracer
	ConnectTracers     []pgx.ConnectTracer
	PoolAcquireTracers []pgxpool.AcquireTracer
	PoolReleaseTracers []pgxpool.ReleaseTracer
}

// New returns new Tracer from tracers with automatically split tracers by interface.
func New(tracers ...pgx.QueryTracer) *Tracer {
	var t Tracer

	for _, tracer := range tracers {
		t.QueryTracers = append(t.QueryTracers, tracer)

		if batchTracer, ok := tracer.(pgx.BatchTracer); ok {
			t.BatchTracers = append(t.BatchTracers, batchTracer)
		}

		if copyFromTracer, ok := tracer.(pgx.CopyFromTracer); ok {
			t.CopyFromTracers = append(t.CopyFromTracers, copyFromTracer)
		}

		if prepareTracer, ok := tracer.(pgx.PrepareTracer); ok {
			t.PrepareTracers = append(t.PrepareTracers, prepareTracer)
		}

		if connectTracer, ok := tracer.(pgx.ConnectTracer); ok {
			t.ConnectTracers = append(t.ConnectTracers, connectTracer)
		}

		if poolAcquireTracer, ok := tracer.(pgxpool.AcquireTracer); ok {
			t.PoolAcquireTracers = append(t.PoolAcquireTracers, poolAcquireTracer)
		}

		if poolReleaseTracer, ok := tracer.(pgxpool.ReleaseTracer); ok {
			t.PoolReleaseTracers = append(t.PoolReleaseTracers, poolReleaseTracer)
		}
	}

	return &t
}

func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for _, tracer := range t.QueryTracers {
		ctx = tracer.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for _, tracer := range t.QueryTracers {
		tracer.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for _, tracer := range t.BatchTracers {
		ctx = tracer.TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	for _, tracer := range t.CopyFromTracers {
		ctx = tracer.TraceCopyFromStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	for _, tracer := range t.CopyFromTracers {
		tracer.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	for _, tracer := range t.PrepareTracers {
		ctx = tracer.TracePrepareStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	for _, tracer := range t.PrepareTracers {
		tracer.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	for _, tracer := range t.ConnectTracers {
		ctx = tracer.TraceConnectStart(ctx, data)
	}

	return ctx
}

func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	for _, tracer := range t.ConnectTracers {
		tracer.TraceConnectEnd(ctx, data)
	}
}

func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	for _, tracer := range t.PoolAcquireTracers {
		ctx = tracer.TraceAcquireStart(ctx, pool, data)
	}

	return ctx
}

func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	for _, tracer := range t.PoolAcquireTracers {
		tracer.TraceAcquireEnd(ctx, pool, data)
	}
}

func (t *Tracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	for _, tracer := range t.PoolReleaseTracers {
		tracer.TraceRelease(pool, data)
	}
}
