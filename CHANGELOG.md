# 4.3.0 (January 23, 2020)

* Fix Rows.Values panic when unable to decode
* Add Rows.Values support for unknown types
* Add DriverContext support for stdlib (Alex Gaynor)
* Update pgproto3 to v2.0.1 to never return an io.EOF as it would be misinterpreted by database/sql. Instead return io.UnexpectedEOF.

# 4.2.1 (January 13, 2020)

* Update pgconn to v1.2.1 (fixes context cancellation data race introduced in v1.2.0))

# 4.2.0 (January 11, 2020)

* Update pgconn to v1.2.0.
* Update pgtype to v1.1.0.
* Return error instead of panic when wrong number of arguments passed to Exec. (malstoun)
* Fix large objects functionality when PreferSimpleProtocol = true.
* Restore GetDefaultDriver which existed in v3. (Johan Brandhorst)
* Add RegisterConnConfig to stdlib which replaces the removed RegisterDriverConfig from v3.

# 4.1.2 (October 22, 2019)

* Fix dbSavepoint.Begin recursive self call
* Upgrade pgtype to v1.0.2 - fix scan pointer to pointer

# 4.1.1 (October 21, 2019)

* Fix pgxpool Rows.CommandTag() infinite loop / typo

# 4.1.0 (October 12, 2019)

## Potentially Breaking Changes

Technically, two changes are breaking changes, but in practice these are extremely unlikely to break existing code.

* Conn.Begin and Conn.BeginTx return a Tx interface instead of the internal dbTx struct. This is necessary for the Conn.Begin method to signature as other methods that begin a transaction.
* Add Conn() to Tx interface. This is necessary to allow code using a Tx to access the *Conn (and pgconn.PgConn) on which the Tx is executing.

## Fixes

* Releasing a busy connection closes the connection instead of returning an unusable connection to the pool
* Do not mutate config.Config.OnNotification in connect

# 4.0.1 (September 19, 2019)

* Fix statement cache cleanup.
* Corrected daterange OID.
* Fix Tx when committing or rolling back multiple times in certain cases.
* Improve documentation.

# 4.0.0 (September 14, 2019)

v4 is a major release with many significant changes some of which are breaking changes. The most significant are
included below.

* Simplified establishing a connection with a connection string.
* All potentially blocking operations now require a context.Context. The non-context aware functions have been removed.
* OIDs are hard-coded for known types. This saves the query on connection.
* Context cancellations while network activity is in progress is now always fatal. Previously, it was sometimes recoverable. This led to increased complexity in pgx itself and in application code.
* Go modules are required.
* Errors are now implemented in the Go 1.13 style.
* `Rows` and `Tx` are now interfaces.
* The connection pool as been decoupled from pgx and is now a separate, included package (github.com/jackc/pgx/v4/pgxpool).
* pgtype has been spun off to a separate package (github.com/jackc/pgtype).
* pgproto3 has been spun off to a separate package (github.com/jackc/pgproto3/v2).
* Logical replication support has been spun off to a separate package (github.com/jackc/pglogrepl).
* Lower level PostgreSQL functionality is now implemented in a separate package (github.com/jackc/pgconn).
* Tests are now configured with environment variables.
* Conn has an automatic statement cache by default.
* Batch interface has been simplified.
* QueryArgs has been removed.
