// Package pgconn is a low-level PostgreSQL database driver.
/*
pgconn provides lower level access to a PostgreSQL connection than a database/sql or pgx connection. It operates at
nearly the same level is the C library libpq.

Establishing a Connection

Use Connect to establish a connection. It accepts a connection string in URL or DSN and will read the environment for
libpq style environment variables.

Executing a Query

ExecParams and ExecPrepared execute a single query. They return readers that iterate over each row. The Read method
reads all rows into memory.

Executing Multiple Queries in a Single Round Trip

Exec and ExecBatch can execute multiple queries in a single round trip. The return readers that iterate over each query
result. The ReadAll method reads all query results into memory.

Context Support

All potentially blocking operations take a context.Context. If a context is canceled while a query is in progress the
method immediately returns. In the background a cancel request will be sent to the PostgreSQL server. If the
cancellation fails or hangs for more than a short time (approximately 15 seconds) the connection will be closed. It is
safe to use the connection while this background cancellation is in progress. Any calls will block until the
cancellation and resynchronization is complete (and those calls can be aborted by a context cancellation).
*/
package pgconn
