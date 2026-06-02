// Package pgconn is a low-level PostgreSQL database driver.
/*
pgconn provides lower level access to a PostgreSQL connection than a database/sql or pgx connection. It operates at
nearly the same level is the C library libpq.

Establishing a Connection

Use Connect to establish a connection. It accepts a connection string in URL or keyword/value format and will read the
environment for libpq style environment variables.

Connecting Securely

By default ParseConfig matches libpq and uses sslmode=prefer, which silently falls back to an unencrypted connection
if the server does not offer TLS. For connections that traverse an untrusted network, set the following parameters
explicitly:

	# URL form
	postgres://user@db.example.com/mydb?sslmode=verify-full&sslrootcert=/path/to/root.crt&channel_binding=require&require_auth=scram-sha-256

	# keyword/value form
	host=db.example.com user=user dbname=mydb sslmode=verify-full sslrootcert=/path/to/root.crt channel_binding=require require_auth=scram-sha-256

	sslmode=verify-full       Require TLS, verify the server certificate against sslrootcert, and verify that the
	                          certificate's host name matches the host being connected to. Weaker modes (disable,
	                          allow, prefer, require) either permit plaintext fallback or skip certificate
	                          verification, allowing a network attacker to impersonate the server.
	channel_binding=require   Require SCRAM-SHA-256-PLUS, which binds the authentication exchange to the TLS channel
	                          so that a TLS-terminating intermediary cannot relay credentials to the real server.
	require_auth=scram-sha-256
	                          Refuse to respond to AuthenticationCleartextPassword or AuthenticationMD5Password
	                          requests from the server. Without this, a server (or interceptor) can request the
	                          password in cleartext and the client will send it.

These parameters may also be set via the PGSSLMODE, PGSSLROOTCERT, PGCHANNELBINDING, and PGREQUIREAUTH environment
variables.

Executing a Query

ExecParams and ExecPrepared execute a single query. They return readers that iterate over each row. The Read method
reads all rows into memory.

Executing Multiple Queries in a Single Round Trip

Exec and ExecBatch can execute multiple queries in a single round trip. They return readers that iterate over each query
result. The ReadAll method reads all query results into memory.

Pipeline Mode

Pipeline mode allows sending queries without having read the results of previously sent queries. It allows control of
exactly how many and when network round trips occur.

Context Support

All potentially blocking operations take a context.Context. The default behavior when a context is canceled is for the
method to immediately return. In most circumstances, this will also close the underlying connection. This behavior can
be customized by using BuildContextWatcherHandler on the Config to create a ctxwatch.Handler with different behavior.
This can be especially useful when queries that are frequently canceled and the overhead of creating new connections is
a problem. DeadlineContextWatcherHandler and CancelRequestContextWatcherHandler can be used to introduce a delay before
interrupting the query in such a way as to close the connection.

The CancelRequest method may be used to request the PostgreSQL server cancel an in-progress query without forcing the
client to abort.
*/
package pgconn
