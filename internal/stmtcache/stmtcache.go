// Package stmtcache is a cache for statement descriptions.
package stmtcache

import (
	"hash/fnv"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

// StatementName returns a statement name that will be stable for sql across multiple connections and program
// executions.
func StatementName(sql string) string {
	h := fnv.New64a()
	h.Write([]byte(sql))
	return "stmtcache_" + strconv.FormatUint(h.Sum64(), 10)
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
