# pgx

PostgreSQL client library for Go

## Description

pgx is a database connection library designed specifically for PostgreSQL. pgx
offers an interface similar to database/sql that offers more performance and
features than are available with the database/sql interface. It also can run as
a database/sql compatible driver.

## Native Interface

The pgx native interface is faster than using pgx as a driver for database/sql.
The performance improvement ranges from as little as 3% for returning a single
value to 30% for returning many rows with multiple columns.

The pgx native interface supports the following extra features:

* Listen / notify
* Transaction isolation level control
* Full TLS connection control
* Binary format support for custom types (can be much faster)
* Logging support
* Configurable connection pool with after connect hooks to do arbitrary connection setup
* PostgreSQL array to Go slice mapping for integers, floats, and strings
* Hstore support
* Large object support

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

To setup the normal test environment run the following SQL:

    create user pgx_md5 password 'secret';
    create database pgx_test;

Connect to database pgx_test and run:

    create extension hstore;

Next open connection_settings_test.go.example and make a copy without the
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

pgx follows semantic versioning for the documented public API. ```master```
branch tracks the latest stable branch (```v2```). Consider using ```import
"gopkg.in/jackc/pgx.v2"``` to lock to the ```v2``` branch or use a vendoring
tool such as [godep](https://github.com/tools/godep).
