[![](https://godoc.org/github.com/jackc/pgx?status.svg)](https://godoc.org/github.com/jackc/pgx)
[![Build Status](https://travis-ci.org/jackc/pgx.svg)](https://travis-ci.org/jackc/pgx)

# pgx - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL. The driver part of pgx is compatible with database/sql but also offers a native
interface similar in style to database/sql that offers better performance and more features.

The toolkit part is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

The current release of pgx `v4` requires Go modules. Checkout and vendor branch `v3` to use the previous version.

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
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
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

It is recommended to use the pgx interface if the application is only targeting PostgreSQL and no other libraries that
require `database/sql` are in use. The pgx interface is faster and exposes more features.

The database/sql interface only allows the underlying driver to return or receive the following types: `int64`,
`float64`, `bool`, `[]byte`, `string`, `time.Time`, or `nil`. The only way to handle other types is to implement the
database/sql.Scanner and the database/sq/driver.Valuer interfaces. These interfaces require using the text format for
transmitting values. The binary format can be substantially faster, and that is what the pgx native interface uses to
encode and decode values for PostgreSQL.

## Features

pgx supports many additional features beyond what is available through database/sql.

* Support for approximately 60 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (can be much faster)
* Copy protocol support for faster bulk data loads
* Extendable logging support including built-in support for log15, logrus, zap, and zerolog
* Connection pool with after connect hook to do arbitrary connection setup
* Listen / notify
* PostgreSQL array to Go slice mapping for integers, floats, and strings
* Hstore support
* JSON and JSONB support
* Maps inet and cidr PostgreSQL types to net.IPNet and net.IP
* Large object support
* NULL mapping to Null* struct or pointer to pointer
* Supports database/sql.Scanner and database/sql/driver.Valuer interfaces for custom types
* Notice response handling (this is different than listen / notify)
* Simulated nested transactions with savepoints

## Performance

There are three areas in particular where pgx can provide a significant performance advantage over the standard
`database/sql` interface and/or other drivers.

1. PostgreSQL specific types - Types such as arrays can be parsed much quicker because pgx uses the binary format.
2. Automatic statement preparation and caching - pgx will prepare and cache statements by default. This can provide an
   significant free improvement to code that does not explicitly use prepared statements. Under certain workloads it
   performs nearly 3x the queries per second.
3. Batched queries - Multiple queries can be batched together to minimize network round trips.

## Related Libraries

pgx is the head of a family of PostgreSQL libraries. Many of these can be used independently. Many can also be accessed
from pgx for lower-level control.

## github.com/jackc/pgconn

pgconn is a lower-level PostgreSQL database driver that operates at nearly the same level as the C library libpq.

## github.com/jackc/pgx/v4/pgxpool

pgxpool is a connection pool for pgx. pgx is entirely decoupled from its default pool implementation. This means pgx can be used with a different pool or without any pool at all.

## github.com/jackc/pgx/v4/stdlib

database/sql compatibility layer for pgx. pgx can be used as a normal database/sql driver, but at any time the native interface may be acquired for more performance or PostgreSQL specific functionality.

## github.com/jackc/pgtype

Over 70 PostgreSQL types are supported including uuid, hstore, json, bytea, numeric, interval, inet, and arrays. These types support database/sql interfaces and are usable even outside of pgx. They are fully tested in pgx and pq. They also support a higher performance interface when used with the pgx driver.

## github.com/jackc/pgproto3

pgproto3 provides standalone encoding and decoding of the PostgreSQL v3 wire protocol. This is useful for implementing very low level PostgreSQL tooling.

## github.com/jackc/pglogrepl

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

## github.com/jackc/pgmock

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

## github.com/jackc/tern

tern is a stand-alone SQL migration system.

## Comparison With Alternatives

* [pq](http://godoc.org/github.com/lib/pq)
* [go-pg](https://github.com/go-pg/pg)

For prepared queries with small result sets of simple data types all drivers perform similarly. If prepared statements
are not being explicitly used, pgx can have a significant performance advantage due to automatic statement preparation.
pgx also can perform better when using PostgreSQL specific data types or query batching. See
[go_db_bench](https://github.com/jackc/go_db_bench) for some database driver benchmarks.

Another significant difference between the drivers is features and API style.

pq is exclusively used with database/sql. go-pg does not use database/sql at all. pgx supports database/sql as well as
its own interface.

go-pg includes many features that traditionally sit above the database driver such as an ORM, struct mapping, soft
deletes, schema migrations, and sharding support baked in. pgx does not and will not include such features in the core package.

## Testing

pgx tests need a PostgreSQL database. It will connect to the database specified in the `PGX_TEST_DATABASE` environment
variable. The `PGX_TEST_DATABASE` environment variable can be a URL or DSN. In addition, the standard `PG*` environment
variables will be respected. Consider using [direnv](https://github.com/direnv/direnv) to simplify environment variable
handling.

### Example Test Environment

Connect to your PostgreSQL server and run:

```
create database pgx_test;
```

Connect to the newly created database and run:

```
create domain uint64 as numeric(20,0);
```

Now you can run the tests:

```
PGX_TEST_DATABASE="host=/var/run/postgresql database=pgx_test" go test ./...
```

In addition there are tests specific for PgBouncer that will be run if the `PGX_TEST_PGBOUNCER_CONN_STRING` is set.

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v4` is the latest stable major version.
