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

	for i := range tracers {
		t.QueryTracers = append(t.QueryTracers, tracers[i])

		if batchTracer, ok := tracers[i].(pgx.BatchTracer); ok {
			t.BatchTracers = append(t.BatchTracers, batchTracer)
		}

		if copyFromTracer, ok := tracers[i].(pgx.CopyFromTracer); ok {
			t.CopyFromTracers = append(t.CopyFromTracers, copyFromTracer)
		}

		if prepareTracer, ok := tracers[i].(pgx.PrepareTracer); ok {
			t.PrepareTracers = append(t.PrepareTracers, prepareTracer)
		}

		if connectTracer, ok := tracers[i].(pgx.ConnectTracer); ok {
			t.ConnectTracers = append(t.ConnectTracers, connectTracer)
		}

		if poolAcquireTracer, ok := tracers[i].(pgxpool.AcquireTracer); ok {
			t.PoolAcquireTracers = append(t.PoolAcquireTracers, poolAcquireTracer)
		}

		if poolReleaseTracer, ok := tracers[i].(pgxpool.ReleaseTracer); ok {
			t.PoolReleaseTracers = append(t.PoolReleaseTracers, poolReleaseTracer)
		}
	}

	return &t
}

func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for i := range t.QueryTracers {
		ctx = t.QueryTracers[i].TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for i := range t.QueryTracers {
		t.QueryTracers[i].TraceQueryEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for i := range t.BatchTracers {
		ctx = t.BatchTracers[i].TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	for i := range t.BatchTracers {
		t.BatchTracers[i].TraceBatchQuery(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for i := range t.BatchTracers {
		t.BatchTracers[i].TraceBatchEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	for i := range t.CopyFromTracers {
		ctx = t.CopyFromTracers[i].TraceCopyFromStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	for i := range t.CopyFromTracers {
		t.CopyFromTracers[i].TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	for i := range t.PrepareTracers {
		ctx = t.PrepareTracers[i].TracePrepareStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	for i := range t.PrepareTracers {
		t.PrepareTracers[i].TracePrepareEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	for i := range t.ConnectTracers {
		ctx = t.ConnectTracers[i].TraceConnectStart(ctx, data)
	}

	return ctx
}

func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	for i := range t.ConnectTracers {
		t.ConnectTracers[i].TraceConnectEnd(ctx, data)
	}
}

func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	for i := range t.PoolAcquireTracers {
		ctx = t.PoolAcquireTracers[i].TraceAcquireStart(ctx, pool, data)
	}

	return ctx
}

func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	for i := range t.PoolAcquireTracers {
		t.PoolAcquireTracers[i].TraceAcquireEnd(ctx, pool, data)
	}
}

func (t *Tracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	for i := range t.PoolReleaseTracers {
		t.PoolReleaseTracers[i].TraceRelease(pool, data)
	}
}
