package pgx

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
)

// QueryTracer traces Query, QueryRow, and Exec.
type QueryTracer interface {
	// TraceQueryStart is called at the beginning of Query, QueryRow, and Exec calls. The returned context is used for the
	// rest of the call and will be passed to TraceQueryEnd.
	TraceQueryStart(ctx context.Context, conn *Conn, data TraceQueryStartData) context.Context

	TraceQueryEnd(ctx context.Context, conn *Conn, data TraceQueryEndData)
}

type TraceQueryStartData struct {
	SQL  string
	Args []any
}

type TraceQueryEndData struct {
	CommandTag pgconn.CommandTag
	Err        error
}

// BatchTracer traces SendBatch.
type BatchTracer interface {
	// TraceBatchStart is called at the beginning of SendBatch calls. The returned context is used for the
	// rest of the call and will be passed to TraceBatchQuery and TraceBatchEnd.
	TraceBatchStart(ctx context.Context, conn *Conn, data TraceBatchStartData) context.Context

	TraceBatchQuery(ctx context.Context, conn *Conn, data TraceBatchQueryData)
	TraceBatchEnd(ctx context.Context, conn *Conn, data TraceBatchEndData)
}

type TraceBatchStartData struct {
	Batch *Batch
}

type TraceBatchQueryData struct {
	SQL        string
	Args       []any
	CommandTag pgconn.CommandTag
	Err        error
}

type TraceBatchEndData struct {
	Err error
}

// CopyFromTracer traces CopyFrom.
type CopyFromTracer interface {
	// TraceCopyFromStart is called at the beginning of CopyFrom calls. The returned context is used for the
	// rest of the call and will be passed to TraceCopyFromEnd.
	TraceCopyFromStart(ctx context.Context, conn *Conn, data TraceCopyFromStartData) context.Context

	TraceCopyFromEnd(ctx context.Context, conn *Conn, data TraceCopyFromEndData)
}

type TraceCopyFromStartData struct {
	TableName   Identifier
	ColumnNames []string
}

type TraceCopyFromEndData struct {
	CommandTag pgconn.CommandTag
	Err        error
}

// PrepareTracer traces Prepare.
type PrepareTracer interface {
	// TracePrepareStart is called at the beginning of Prepare calls. The returned context is used for the
	// rest of the call and will be passed to TracePrepareEnd.
	TracePrepareStart(ctx context.Context, conn *Conn, data TracePrepareStartData) context.Context

	TracePrepareEnd(ctx context.Context, conn *Conn, data TracePrepareEndData)
}

type TracePrepareStartData struct {
	Name string
	SQL  string
}

type TracePrepareEndData struct {
	AlreadyPrepared bool
	Err             error
}

// ConnectTracer traces Connect and ConnectConfig.
type ConnectTracer interface {
	// TraceConnectStart is called at the beginning of Connect and ConnectConfig calls. The returned context is used for
	// the rest of the call and will be passed to TraceConnectEnd.
	TraceConnectStart(ctx context.Context, data TraceConnectStartData) context.Context

	TraceConnectEnd(ctx context.Context, data TraceConnectEndData)
}

type TraceConnectStartData struct {
	ConnConfig *ConnConfig
}

type TraceConnectEndData struct {
	Conn *Conn
	Err  error
}

type MultiTracer struct {
	QueryTracers    []QueryTracer
	BatchTracers    []BatchTracer
	CopyFromTracers []CopyFromTracer
	PrepareTracers  []PrepareTracer
	ConnectTracers  []ConnectTracer
}

func NewMultiTracer(tracers ...QueryTracer) *MultiTracer {
	var t MultiTracer

	for _, tracer := range tracers {
		t.QueryTracers = append(t.QueryTracers, tracer)

		if batchTracer, ok := tracer.(BatchTracer); ok {
			t.BatchTracers = append(t.BatchTracers, batchTracer)
		}

		if copyFromTracer, ok := tracer.(CopyFromTracer); ok {
			t.CopyFromTracers = append(t.CopyFromTracers, copyFromTracer)
		}

		if prepareTracer, ok := tracer.(PrepareTracer); ok {
			t.PrepareTracers = append(t.PrepareTracers, prepareTracer)
		}

		if connectTracer, ok := tracer.(ConnectTracer); ok {
			t.ConnectTracers = append(t.ConnectTracers, connectTracer)
		}
	}

	return &t
}

func (t *MultiTracer) TraceQueryStart(ctx context.Context, conn *Conn, data TraceQueryStartData) context.Context {
	for _, tracer := range t.QueryTracers {
		ctx = tracer.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (t *MultiTracer) TraceQueryEnd(ctx context.Context, conn *Conn, data TraceQueryEndData) {
	for _, tracer := range t.QueryTracers {
		tracer.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *MultiTracer) TraceBatchStart(ctx context.Context, conn *Conn, data TraceBatchStartData) context.Context {
	for _, tracer := range t.BatchTracers {
		ctx = tracer.TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (t *MultiTracer) TraceBatchQuery(ctx context.Context, conn *Conn, data TraceBatchQueryData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *MultiTracer) TraceBatchEnd(ctx context.Context, conn *Conn, data TraceBatchEndData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *MultiTracer) TraceCopyFromStart(ctx context.Context, conn *Conn, data TraceCopyFromStartData) context.Context {
	for _, tracer := range t.CopyFromTracers {
		ctx = tracer.TraceCopyFromStart(ctx, conn, data)
	}

	return ctx
}

func (t *MultiTracer) TraceCopyFromEnd(ctx context.Context, conn *Conn, data TraceCopyFromEndData) {
	for _, tracer := range t.CopyFromTracers {
		tracer.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *MultiTracer) TracePrepareStart(ctx context.Context, conn *Conn, data TracePrepareStartData) context.Context {
	for _, tracer := range t.PrepareTracers {
		ctx = tracer.TracePrepareStart(ctx, conn, data)
	}

	return ctx
}

func (t *MultiTracer) TracePrepareEnd(ctx context.Context, conn *Conn, data TracePrepareEndData) {
	for _, tracer := range t.PrepareTracers {
		tracer.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *MultiTracer) TraceConnectStart(ctx context.Context, data TraceConnectStartData) context.Context {
	for _, tracer := range t.ConnectTracers {
		ctx = tracer.TraceConnectStart(ctx, data)
	}

	return ctx
}

func (t *MultiTracer) TraceConnectEnd(ctx context.Context, data TraceConnectEndData) {
	for _, tracer := range t.ConnectTracers {
		tracer.TraceConnectEnd(ctx, data)
	}
}
