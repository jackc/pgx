[![](https://godoc.org/github.com/jackc/pgx?status.svg)](https://pkg.go.dev/github.com/jackc/pgx/v5)
![Build Status](https://github.com/jackc/pgx/actions/workflows/ci.yml/badge.svg)

# pgx - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL.

The pgx driver is a low-level, high performance interface that exposes PostgreSQL-specific features such as `LISTEN` /
`NOTIFY` and `COPY`. It also includes an adapter for the standard `database/sql` interface.

The toolkit component is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

## Example Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
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

See the [getting started guide](https://github.com/jackc/pgx/wiki/Getting-started-with-pgx) for more information.

## Features

* Support for approximately 70 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (allows for much quicker encoding/decoding)
* `COPY` protocol support for faster bulk data loads
* Tracing and logging support
* Connection pool with after-connect hook for arbitrary connection setup
* `LISTEN` / `NOTIFY`
* Conversion of PostgreSQL arrays to Go slice mappings for integers, floats, and strings
* `hstore` support
* `json` and `jsonb` support
* Maps `inet` and `cidr` PostgreSQL types to `netip.Addr` and `netip.Prefix`
* Large object support
* NULL mapping to pointer to pointer
* Supports `database/sql.Scanner` and `database/sql/driver.Valuer` interfaces for custom types
* Notice response handling
* Simulated nested transactions with savepoints

## Choosing Between the pgx and database/sql Interfaces

The pgx interface is faster. Many PostgreSQL specific features such as `LISTEN` / `NOTIFY` and `COPY` are not available
through the `database/sql` interface.

The pgx interface is recommended when:

1. The application only targets PostgreSQL.
2. No other libraries that require `database/sql` are in use.

It is also possible to use the `database/sql` interface and convert a connection to the lower-level pgx interface as needed.

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

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.18 and higher and PostgreSQL 10 and higher. pgx also is tested against the latest version of [CockroachDB](https://www.cockroachlabs.com/product/).

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v5` is the latest stable major version.

## PGX Family Libraries

### [github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

### [github.com/jackc/pgmock](https://github.com/jackc/pgmock)

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

### [github.com/jackc/tern](https://github.com/jackc/tern)

tern is a stand-alone SQL migration system.

## Adapters for 3rd Party Types

* [github.com/jackc/pgx-gofrs-uuid](https://github.com/jackc/pgx-gofrs-uuid)
* [github.com/jackc/pgx-shopspring-decimal](https://github.com/jackc/pgx-shopspring-decimal)
* [github.com/vgarvardt/pgx-google-uuid](https://github.com/vgarvardt/pgx-google-uuid)

## Adapters for 3rd Party Loggers

These adapters can be used with the tracelog package.

* [github.com/jackc/pgx-go-kit-log](https://github.com/jackc/pgx-go-kit-log)
* [github.com/jackc/pgx-log15](https://github.com/jackc/pgx-log15)
* [github.com/jackc/pgx-logrus](https://github.com/jackc/pgx-logrus)
* [github.com/jackc/pgx-zap](https://github.com/jackc/pgx-zap)
* [github.com/jackc/pgx-zerolog](https://github.com/jackc/pgx-zerolog)

## 3rd Party Libraries with PGX Support

### [github.com/georgysavva/scany](https://github.com/georgysavva/scany)

Library for scanning data from a database into Go structs and more.

### [https://github.com/otan/gopgkrb5](https://github.com/otan/gopgkrb5)

Adds GSSAPI / Kerberos authentication support.
