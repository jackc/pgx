# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgx is a PostgreSQL driver and toolkit for Go (`github.com/jackc/pgx/v5`). It provides both a native PostgreSQL interface and a `database/sql` compatible driver. Requires Go 1.24+ and supports PostgreSQL 14+ and CockroachDB.

## Build & Test Commands

```bash
# Run all tests (requires PGX_TEST_DATABASE to be set)
go test ./...

# Run a specific test
go test -run TestFunctionName ./...

# Run tests for a specific package
go test ./pgconn/...

# Run tests with race detector
go test -race ./...

# DevContainer: run tests against specific PostgreSQL versions
./test.sh pg18                      # Default: PostgreSQL 18
./test.sh pg16 -run TestConnect     # Specific test against PG16
./test.sh crdb                      # CockroachDB
./test.sh all                       # All targets (pg14-18 + crdb)

# Format
goimports -w .

# Lint
golangci-lint run ./...
```

## Test Database Setup

Tests require `PGX_TEST_DATABASE` environment variable. In the devcontainer, `test.sh` handles this. For local development:

```bash
export PGX_TEST_DATABASE="host=localhost user=postgres password=postgres dbname=pgx_test"
```

The test database needs extensions: `hstore`, `ltree`, and a `uint64` domain. See `testsetup/postgresql_setup.sql` for full setup. Many tests are skipped unless additional `PGX_TEST_*` env vars are set (for TLS, SCRAM, MD5, unix socket, PgBouncer testing).

## Architecture

The codebase is a layered architecture, bottom-up:

- **pgproto3/** — PostgreSQL wire protocol v3 encoder/decoder. Defines `FrontendMessage` and `BackendMessage` types for every protocol message.
- **pgconn/** — Low-level connection layer (roughly libpq-equivalent). Handles authentication, TLS, query execution, COPY protocol, and notifications. `PgConn` is the core type.
- **pgx** (root package) — High-level query interface built on `pgconn`. Provides `Conn`, `Rows`, `Tx`, `Batch`, `CopyFrom`, and generic helpers like `CollectRows`/`ForEachRow`. Includes automatic statement caching (LRU).
- **pgtype/** — Type system mapping between Go and PostgreSQL types (70+ types). Key interfaces: `Codec`, `Type`, `TypeMap`. Custom types (enums, composites, domains) are registered through `TypeMap`.
- **pgxpool/** — Concurrency-safe connection pool built on `puddle/v2`. `Pool` is the main type; wraps `pgx.Conn`.
- **stdlib/** — `database/sql` compatibility adapter.

Supporting packages:
- **internal/stmtcache/** — Prepared statement cache with LRU eviction
- **internal/sanitize/** — SQL query sanitization
- **tracelog/** — Logging adapter that implements tracer interfaces
- **multitracer/** — Composes multiple tracers into one
- **pgxtest/** — Test helpers for running tests across connection types

## Key Design Conventions

- **Semantic versioning** — strictly followed. Do not break the public API (no removing or renaming exported types, functions, methods, or fields; no changing function signatures).
- **Minimal dependencies** — adding new dependencies is strongly discouraged (see CONTRIBUTING.md).
- **Context-based** — all blocking operations take `context.Context`.
- **Tracer interfaces** — observability via `QueryTracer`, `BatchTracer`, `CopyFromTracer`, `PrepareTracer` on `ConnConfig.Tracer`.
- **Formatting** — use `goimports -w .` to format code. CI checks formatting via `gofmt -l -s -w . && git diff --exit-code`. `gofumpt` with extra rules is also enforced via `golangci-lint`.
- **Linters** — `govet` and `ineffassign` only (configured in `.golangci.yml`).
- **CI matrix** — tests run against Go 1.24/1.25 × PostgreSQL 14-18 + CockroachDB, on Linux and Windows. Race detector enabled on Linux only.
