# Pgx

Pgx is a pure Go database connection library designed specifically for
PostgreSQL. Pgx is different from other drivers such as
[pq](http://godoc.org/github.com/lib/pq) because, while it can operate as a
database/sql compatible driver, pgx is primarily intended to be used directly.
It offers a native interface similar to database/sql that offers better
performance and more features.

## Features

Pgx supports many additional features beyond what is available through database/sql.

* Listen / notify
* Transaction isolation level control
* Full TLS connection control
* Binary format support for custom types (can be much faster)
* Copy protocol support for faster bulk data loads
* Logging support
* Configurable connection pool with after connect hooks to do arbitrary connection setup
* PostgreSQL array to Go slice mapping for integers, floats, and strings
* Hstore support
* JSON and JSONB support
* Maps inet and cidr PostgreSQL types to net.IPNet and net.IP
* Large object support
* Null mapping to Null* struct or pointer to pointer.
* Supports database/sql.Scanner and database/sql/driver.Valuer interfaces for custom types

## Performance

Pgx performs roughly equivalent to [pq](http://godoc.org/github.com/lib/pq) and
[go-pg](https://github.com/go-pg/pg) for selecting a single column from a single
row, but it is substantially faster when selecting multiple entire rows (6893
queries/sec for pgx vs. 3968 queries/sec for pq -- 73% faster).

See this [gist](https://gist.github.com/jackc/d282f39e088b495fba3e) for the
underlying benchmark results or checkout
[go_db_bench](https://github.com/jackc/go_db_bench) to run tests for yourself.

## database/sql

Import the ```github.com/jackc/pgx/stdlib``` package to use pgx as a driver for
database/sql. It is possible to retrieve a pgx connection from database/sql on
demand. This allows using the database/sql interface in most places, but using
pgx directly when more performance or PostgreSQL specific features are needed.

## Documentation

pgx includes extensive documentation in the godoc format. It is viewable online at [godoc.org](https://godoc.org/github.com/jackc/pgx).

## Testing

pgx supports multiple connection and authentication types. Setting up a test
environment that can test all of them can be cumbersome. In particular,
Windows cannot test Unix domain socket connections. Because of this pgx will
skip tests for connection types that are not configured.

### Normal Test Environment

To setup the normal test environment, first install these dependencies:

    go get github.com/jackc/fake
    go get github.com/shopspring/decimal
    go get gopkg.in/inconshreveable/log15.v2

Then run the following SQL:

    create user pgx_md5 password 'secret';
    create database pgx_test;

Connect to database pgx_test and run:

    create extension hstore;

Next open conn_config_test.go.example and make a copy without the
.example. If your PostgreSQL server is accepting connections on 127.0.0.1,
then you are done.

### Connection and Authentication Test Environment

Complete the normal test environment setup and also do the following.

Run the following SQL:

    create user pgx_none;
    create user pgx_pw password 'secret';

Add the following to your pg_hba.conf:

If you are developing on Unix with domain socket connections:

    local  pgx_test  pgx_none  trust
    local  pgx_test  pgx_pw    password
    local  pgx_test  pgx_md5   md5

If you are developing on Windows with TCP connections:

    host  pgx_test  pgx_none  127.0.0.1/32 trust
    host  pgx_test  pgx_pw    127.0.0.1/32 password
    host  pgx_test  pgx_md5   127.0.0.1/32 md5

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. Branch ```v2``` is the latest stable release. ```master``` can contain new features or behavior that will change or be removed before being merged to the stable ```v2``` branch (in practice, this occurs very rarely).

Consider using a vendoring
tool such as [godep](https://github.com/tools/godep) or importing pgx via ```import
"gopkg.in/jackc/pgx.v2"``` to lock to the ```v2``` branch.
