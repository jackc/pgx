package pgx

import (
	"context"
	"time"

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

	// ReadStartTime is when pgx began reading this query's result from the server -- i.e. when it requested the next
	// result in the batch, not when SendBatch was called or when this query's turn in the batch started.
	//
	// The interval from ReadStartTime to when TraceBatchQuery is called is not a pure network or server measurement.
	// It excludes any time the query spent waiting for earlier queries in the batch to be consumed, but it does
	// include network waits, server-side buffering, and -- for a query consumed via [QueuedQuery.Query] or
	// [BatchResults.Query] -- however long the calling application takes to read and close the returned Rows, since
	// TraceBatchQuery for that query is not called until Rows is closed.
	ReadStartTime time.Time
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

// ConnectTracer traces Connect, ConnectConfig, and Connect2.
type ConnectTracer interface {
	// TraceConnectStart is called at the beginning of Connect, ConnectConfig, and Connect2 calls. The returned context is used for
	// the rest of the call and will be passed to TraceConnectEnd.
	TraceConnectStart(ctx context.Context, data TraceConnectStartData) context.Context

	TraceConnectEnd(ctx context.Context, data TraceConnectEndData)
}

type TraceConnectStartData struct {
	// For Connect2, this is an independent input snapshot with Connect2String and
	// Connect2Options populated. Legacy connection fields are empty and execution
	// settings are not resolved yet. Mutations do not affect connection establishment.
	ConnConfig *ConnConfig
}

type TraceConnectEndData struct {
	Conn *Conn
	Err  error
}
