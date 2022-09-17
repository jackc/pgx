// Package stmtcache is a cache for statement descriptions.
package stmtcache

import (
	"strconv"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
)

var stmtCounter int64

// NextStatementName returns a statement name that will be unique for the lifetime of the program.
func NextStatementName() string {
	n := atomic.AddInt64(&stmtCounter, 1)
	return "stmtcache_" + strconv.FormatInt(n, 10)
}

// Cache caches statement descriptions.
type Cache interface {
	// Get returns the statement description for sql. Returns nil if not found.
	Get(sql string) *pgconn.StatementDescription

	// Put stores sd in the cache. Put panics if sd.SQL is "". Put does nothing if sd.SQL already exists in the cache.
	Put(sd *pgconn.StatementDescription)

	// Invalidate invalidates statement description identified by sql. Does nothing if not found.
	Invalidate(sql string)

	// InvalidateAll invalidates all statement descriptions.
	InvalidateAll()

	// HandleInvalidated returns a slice of all statement descriptions invalidated since the last call to HandleInvalidated.
	HandleInvalidated() []*pgconn.StatementDescription

	// Len returns the number of cached prepared statement descriptions.
	Len() int

	// Cap returns the maximum number of cached prepared statement descriptions.
	Cap() int
}

func IsStatementInvalid(err error) bool {
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		return false
	}

	// https://github.com/jackc/pgx/issues/1162
	//
	// We used to look for the message "cached plan must not change result type". However, that message can be localized.
	// Unfortunately, error code "0A000" - "FEATURE NOT SUPPORTED" is used for many different errors and the only way to
	// tell the difference is by the message. But all that happens is we clear a statement that we otherwise wouldn't
	// have so it should be safe.
	possibleInvalidCachedPlanError := pgErr.Code == "0A000"
	return possibleInvalidCachedPlanError
}
