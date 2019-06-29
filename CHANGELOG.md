# 4.0.0 (Prerelease)

v4 is a major release with many significant changes.

* Simplified establishing a connection with a connection string.
* All potentially blocking operations now require a context.Context. The non-context aware functions have been removed.
* OIDs are hard-coded for known types. This saves the query on connection.
* Context cancellations while network activity is in progress is now always fatal. Previously, it was sometimes recoverable. This led to increased complexity in pgx itself and in application code.
* Go modules are required.
* Errors are now implemented in the Go 1.13 style. Tests for specific error types must use `As` or `Is`.
* `Rows` is now an interface.
* The connection pool as been decoupled from pgx and is now a separate, included package (github.com/jackc/pgx/v4/pgxpool).
* pgtype has been spun off to a separate package (github.com/jackc/pgtype).
* pgproto3 has been spun off to a separate package (github.com/jackc/pgproto3).
* Lower level PostgreSQL functionality is now implemented in a separate package (github.com/jackc/pgconn).
* Replication support has been temporarily removed. It will be implemented in a separate package.
* Tests are now configured with environment variables.
