package pgxpool

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// AcquireTracer traces Acquire.
type AcquireTracer interface {
	// TraceAcquireStart is called at the beginning of Acquire.
	// The returned context is used for the rest of the call and will be passed to the TraceAcquireEnd.
	TraceAcquireStart(ctx context.Context, data TraceAcquireStartData) context.Context
	TraceAcquireEnd(ctx context.Context, data TraceAcquireEndData)
}

type TraceAcquireStartData struct {
	ConnConfig *pgx.ConnConfig
}

type TraceAcquireEndData struct {
	Err error
}
