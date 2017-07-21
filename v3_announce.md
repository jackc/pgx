Ann: pgx v3 - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL. pgx supports database/sql
while also offering optionally offering better performance and PostgreSQL
specific features by bypassing the database/sql abstraction.

https://github.com/jackc/pgx

## Features

pgx supports many additional features beyond what is available through database/sql.

* Support for approximately 60 different PostgreSQLtypes
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (can be much faster)
* Copy protocol support for faster bulk data loads
* Extendable logging support including builtin support for log15 and logrus
* Configurable connection pool with after connect hooks to do arbitrary connection setup
* Listen / notify
* Transaction isolation level control
* PostgreSQL array to Go slice mapping for integers, floats, and strings
* Hstore support
* JSON and JSONB support
* Maps inet and cidr PostgreSQL types to net.IPNet and net.IP
* Large object support
* NULL mapping to Null* struct or pointer to pointer.
* Supports database/sql.Scanner and database/sql/driver.Valuer interfaces for custom types
* Logical replication connections, including receiving WAL and sending standby status updates
* Notice response handling (this is different than listen / notify)

## Performance

pgx performs roughly equivalent to [go-pg](https://github.com/go-pg/pg) and is
almost always faster than [pq](http://godoc.org/github.com/lib/pq). When parsing
large result sets the percentage difference can be significant (16483
queries/sec for pgx vs. 10106 queries/sec for pq -- 63% faster).

In many use cases a significant cause of latency is network round trips between
the application and the server. Pgx supports query batching to bundle multiple
queries into a single round trip. Even in the case of the fastest possible
connection, a local Unix domain socket, batching as few as three queries
together can yield an improvement of 57%. With a typical network connection the
results can be even more substantial.

See this [gist](https://gist.github.com/jackc/4996e8648a0c59839bff644f49d6e434)
for the underlying benchmark results or checkout
[go_db_bench](https://github.com/jackc/go_db_bench) to run tests for yourself.

In addition to the native driver, pgx also includes a number of packages that
provide additional functionality.

## stdlib

database/sql compatibility layer for pgx. pgx can be used as a normal database/sql driver, but at any time the native interface may be acquired for more performance or PostgreSQL specific functionality

## pgtype

Approximately 60 PostgreSQL types are supported including uuid, hstore, json, bytea, numeric, interval, inet, and arrays. These types support database/sql interfaces and are usable even outside of pgx. They are fully tested in pgx and lib/pq. They also support a higher performance interface when used with the pgx driver.

## pgproto3

pgproto3 provides standalone encoding and decoding of the PostgreSQL v3 wire protocol. This is useful for implementing very low level PostgreSQL tooling.

## pgmock

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

https://github.com/jackc/pgx

Jack
