[![](https://godoc.org/github.com/jackc/pgconn?status.svg)](https://godoc.org/github.com/jackc/pgconn)
[![Build Status](https://travis-ci.org/jackc/pgconn.svg)](https://travis-ci.org/jackc/pgconn)

# pgconn

Package pgconn is a low-level PostgreSQL database driver.

It is intended to serve as the foundation for the next generation of https://github.com/jackc/pgx.

## Testing

pgconn tests need a PostgreSQL database. It will connect to the database specified in the `PGX_TEST_CONN_STRING`
environment variable. The `PGX_TEST_CONN_STRING` environment variable can be a URL or DSN. In addition, the standard `PG*`
environment variables will be respected. Consider using [direnv](https://github.com/direnv/direnv) to simplify
environment variable handling.

### Example Test Environment

Connect to your PostgreSQL server and run:

```
create database pgx_test;
```

Now you can run the tests:

```
PGX_TEST_CONN_STRING="host=/var/run/postgresql database=pgx_test" go test ./...
```

### Connection and Authentication Tests

There are multiple connection types and means of authentication that pgconn supports. These tests are optional. They
will only run if the appropriate environment variable is set. Run `go test -v | grep SKIP` to see if any tests are being
skipped. Typical developers will not need to enable these tests. See travis.yml for example setup if you need change
authentication code.
