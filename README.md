[![](https://godoc.org/github.com/jackc/pgx?status.svg)](https://pkg.go.dev/github.com/jackc/pgx/v4)
[![Build Status](https://travis-ci.org/jackc/pgx.svg)](https://travis-ci.org/jackc/pgx)

# pgx - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL.

pgx aims to be low-level, fast, and performant, while also enabling PostgreSQL-specific features that the standard `database/sql` package does not allow for.

The driver component of pgx can be used alongside the standard `database/sql` package.

The toolkit component is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

The current release of `pgx v4` requires Go modules. To use the previous version, checkout and vendor the `v3` branch.

## Example Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	// The format for "DATABASE_URL" is here:
	// https://godoc.org/github.com/jackc/pgconn#ParseConfig
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(name, weight)
}
```

## Choosing Between the pgx and database/sql Interfaces

It is recommended to use the pgx interface if:
1. The application only targets PostgreSQL.
2. No other libraries that require `database/sql` are in use.

The pgx interface is faster and exposes more features.

The `database/sql` interface only allows the underlying driver to return or receive the following types: `int64`,
`float64`, `bool`, `[]byte`, `string`, `time.Time`, or `nil`. Handling other types requires implementing the
`database/sql.Scanner` and the `database/sql/driver/driver.Valuer` interfaces which require transmission of values in text format. The binary format can be substantially faster, which is what the pgx interface uses.

## Features

pgx supports many features beyond what is available through `database/sql`:

* Support for approximately 70 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (allows for much quicker encoding/decoding)
* Copy protocol support for faster bulk data loads
* Extendable logging support including built-in support for `log15adapter`, [`logrus`](https://github.com/sirupsen/logrus), [`zap`](https://github.com/uber-go/zap), and [`zerolog`](https://github.com/rs/zerolog)
* Connection pool with after-connect hook for arbitrary connection setup
* Listen / notify
* Conversion of PostgreSQL arrays to Go slice mappings for integers, floats, and strings
* Hstore support
* JSON and JSONB support
* Maps `inet` and `cidr` PostgreSQL types to `net.IPNet` and `net.IP`
* Large object support
* NULL mapping to Null* struct or pointer to pointer
* Supports `database/sql.Scanner` and `database/sql/driver.Valuer` interfaces for custom types
* Notice response handling
* Simulated nested transactions with savepoints

## PgBouncer & Common Gotchas

* Think about how your application uses the database. Unless you know for sure that there will only be one database request at a time (e.g. no goroutines), then **you must use [PgBouncer](https://www.pgbouncer.org/) on top of PostgreSQL** and on top of pgx. (That, or some other pooling mechanism, like pgxpool.)
* This is because pgx is only designed to be used by **one process/thread at a time**, unlike the other Golang SQL drivers that you may be used to. Without a pooling mechanism, you will quickly start to get "conn busy" errors, as documented in [issue #635](https://github.com/jackc/pgx/issues/635). Brandur has [an excellent blog post](https://brandur.org/postgres-connections) explaining why using PgBouncer is really important. Even for a small web-app, you need to use PgBouncer.
* In order to use PgBouncer with pgx, you must include `statement_cache_mode=describe` in your DSN connection string. This is described in more detail in [issue #650](https://github.com/jackc/pgx/issues/650).

## Performance

There are three areas in particular where pgx can provide a significant performance advantage over the standard
`database/sql` interface and other drivers:

1. PostgreSQL specific types - Types such as arrays can be parsed much quicker because pgx uses the binary format.
2. Automatic statement preparation and caching - pgx will prepare and cache statements by default. This can provide an
   significant free improvement to code that does not explicitly use prepared statements. Under certain workloads, it can
   perform nearly 3x the number of queries per second.
3. Batched queries - Multiple queries can be batched together to minimize network round trips.

## Comparison with Alternatives

* [pq](http://godoc.org/github.com/lib/pq)
* [go-pg](https://github.com/go-pg/pg)

For prepared queries with small sets of simple data types, all drivers will have have similar performance. However, if prepared statements aren't being explicitly used, pgx can have a significant performance advantage due to automatic statement preparation.
pgx also can perform better when using PostgreSQL-specific data types or query batching. See
[go_db_bench](https://github.com/jackc/go_db_bench) for some database driver benchmarks.

### Compatibility with `database/sql`

pq is exclusively used with `database/sql`. go-pg does not use `database/sql` at all. pgx supports `database/sql` as well as
its own interface.

### Level of access, ORM

go-pg is a PostgreSQL client and ORM. It includes many features that traditionally sit above the database driver, such as ORM, struct mapping, soft deletes, schema migrations, and sharding support.

pgx is "closer to the metal" and such abstractions are beyond the scope of the pgx project, which first and foremost, aims to be a performant driver and toolkit.

## Testing

pgx tests naturally require a PostgreSQL database. It will connect to the database specified in the `PGX_TEST_DATABASE` environment
variable. The `PGX_TEST_DATABASE` environment variable can either be a URL or DSN. In addition, the standard `PG*` environment
variables will be respected. Consider using [direnv](https://github.com/direnv/direnv) to simplify environment variable
handling.

### Example Test Environment

Connect to your PostgreSQL server and run:

```
create database pgx_test;
```

Connect to the newly-created database and run:

```
create domain uint64 as numeric(20,0);
```

Now, you can run the tests:

```
PGX_TEST_DATABASE="host=/var/run/postgresql database=pgx_test" go test ./...
```

In addition, there are tests specific for PgBouncer that will be executed if `PGX_TEST_PGBOUNCER_CONN_STRING` is set.

## Supported Go and PostgreSQL Versions

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.13 and higher and PostgreSQL 9.5 and higher.

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v4` is the latest stable major version.

## Related Libraries

pgx is the head of a family of PostgreSQL libraries. Many of these can be used independently. Many can also be accessed
from pgx for lower-level control.

### [github.com/jackc/pgconn](https://github.com/jackc/pgconn)

`pgconn` is a lower-level PostgreSQL database driver that operates at nearly the same level as the C library `libpq`.

### [github.com/jackc/pgx/v4/pgxpool](https://github.com/jackc/pgx/tree/master/pgxpool)

`pgxpool` is a connection pool for pgx. pgx is entirely decoupled from its default pool implementation. This means that pgx can be used with a different pool. or without any pool at all.

### [github.com/jackc/pgx/v4/stdlib](https://github.com/jackc/pgx/tree/master/stdlib)

This is a `database/sql` compatibility layer for pgx. pgx can be used as a normal `database/sql` driver, but at any time, the native interface can be acquired for more performance or PostgreSQL specific functionality.

### [github.com/jackc/pgtype](https://github.com/jackc/pgtype)

Over 70 PostgreSQL types are supported including `uuid`, `hstore`, `json`, `bytea`, `numeric`, `interval`, `inet`, and arrays. These types support `database/sql` interfaces and are usable outside of pgx. They are fully tested in pgx and pq. They also support a higher performance interface when used with the pgx driver.

### [github.com/jackc/pgproto3](https://github.com/jackc/pgproto3)

pgproto3 provides standalone encoding and decoding of the PostgreSQL v3 wire protocol. This is useful for implementing very low level PostgreSQL tooling.

### [github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

### [github.com/jackc/pgmock](https://github.com/jackc/pgmock)

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

### [github.com/jackc/tern](https://github.com/jackc/tern)

tern is a stand-alone SQL migration system.

### [github.com/jackc/pgerrcode](https://github.com/jackc/pgerrcode)

pgerrcode contains constants for the PostgreSQL error codes.
