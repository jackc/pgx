package pgxpool

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// AcquireTracer traces Acquire.
type AcquireTracer interface {
	// TraceAcquireStart is called at the beginning of Acquire.
	// The returned context is used for the rest of the call and will be passed to the TraceAcquireEnd.
	TraceAcquireStart(ctx context.Context, pool *Pool, data TraceAcquireStartData) context.Context
	// TraceAcquireEnd is called when a connection has been acquired.
	TraceAcquireEnd(ctx context.Context, pool *Pool, data TraceAcquireEndData)
}

type TraceAcquireStartData struct{}

type TraceAcquireEndData struct {
	Conn *pgx.Conn
	Err  error
}

// ReleaseTracer traces Release.
type ReleaseTracer interface {
	// TraceReleaseStart is called at the beginning of Release.
	// The returned context will be passed to the TraceReleaseEnd.
	TraceReleaseStart(ctx context.Context, pool *Pool, data TraceReleaseStartData) context.Context
	// TraceReleaseEnd is called when a connection has been released.
	TraceReleaseEnd(ctx context.Context, pool *Pool, data TraceReleaseEndData)
}

type TraceReleaseStartData struct {
	Conn *pgx.Conn
}

type TraceReleaseEndData struct{}
