# Contributing

## Discuss Significant Changes

Before you invest a significant amount of time on a change, please create a discussion or issue describing your
proposal. This will help to ensure your proposed change has a reasonable chance of being merged.

## Avoid Dependencies

Adding a dependency is a big deal. While on occasion a new dependency may be accepted, the default answer to any change
that adds a dependency is no.

## AI

Using AI is acceptable (not that it can really be stopped) under one the following conditions.

* AI was used, but you deeply understand the code and you can answer questions regarding your change. You are not going
  to answer questions with "I don't know", AI did it. You are not going to "answer" questions by relaying them to your
  agent. This is wasteful of the code reviewer's time.
* AI was used to solve a problem without your deep understanding. This can still be a good starting point for a fix or
  feature. But you need to clearly state that this is an AI proposal. You should include additional information such as
  the AI used and what prompts were used. You should also be aware that large, complicated, or subtle changes may be
  rejected simply because the reviewer is not confident in a change that no human understands.

## Development Environment Setup

pgx tests naturally require a PostgreSQL database. It will connect to the database specified in the
`PGX_TEST_DATABASE` environment variable. The `PGX_TEST_DATABASE` environment variable can either be
a URL or key-value pairs. In addition, the standard `PG*` environment variables will be respected.

### The full environment

[DEVELOPMENT.md](DEVELOPMENT.md) describes the maintained setup: [mise](https://mise.jdx.dev)
installs the toolchain, and each checkout has its own PostgreSQL 14-18 clusters and CockroachDB
node, so the whole test matrix is available locally. PostgreSQL 18 stays running; other servers
start and stop around their tests. It works natively on macOS and Linux, and in the included
devcontainer.

```
mise install
mise run dev:init
mise run dev
./test.sh all
```

### Using an existing PostgreSQL cluster

If you already have a PostgreSQL development server this is the quickest way to run the majority of
the pgx test suite, and it needs nothing from the section above. Some tests will be skipped that
require server configuration changes (e.g. those testing different authentication methods).

Create and setup a test database:

```
export PGDATABASE=pgx_test
createdb
psql -c 'create extension hstore;'
psql -c 'create extension ltree;'
psql -c 'create domain uint64 as numeric(20,0);'
```

Ensure a `postgres` user exists. This happens by default in normal PostgreSQL installs, but some
installation methods such as Homebrew do not.

```
createuser -s postgres
```

Ensure your `PGX_TEST_DATABASE` environment variable points to the database you just created and run
the tests.

```
export PGX_TEST_DATABASE="host=/private/tmp database=pgx_test"
go test ./...
```

This will run the vast majority of the tests, but some tests will be skipped (e.g. those testing
different connection methods).

### PgBouncer

There are tests specific for PgBouncer that will be executed if `PGX_TEST_PGBOUNCER_CONN_STRING` is set.
The test PgBouncer must be version 1.21.0 or newer, use transaction pooling, and have `max_prepared_statements` set to a
non-zero value. This ensures the tests cover PgBouncer's protocol-level named prepared statement support in addition to
the pgx query modes that do not use named prepared statements.

### Optional Tests

pgx supports multiple connection types and means of authentication. These tests are optional. They will only run if the
appropriate environment variables are set. In addition, there may be tests specific to particular PostgreSQL versions,
non-PostgreSQL servers (e.g. CockroachDB), or connection poolers (e.g. PgBouncer). `go test ./... -v | grep SKIP` to see
if any tests are being skipped.
