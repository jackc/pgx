# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgx is a PostgreSQL driver and toolkit for Go (`github.com/jackc/pgx/v5`). It provides both a native PostgreSQL interface and a `database/sql` compatible driver. Requires Go 1.25+ and supports PostgreSQL 14+ and CockroachDB.

## Build & Test Commands

Every checkout has its own PostgreSQL 14-18 and CockroachDB instances, supervised by
process-compose. `mise run dev` starts PostgreSQL 18; tests start other targets on demand and stop
them afterwards unless they were explicitly prewarmed. See DEVELOPMENT.md.

```bash
mise run dev                        # start PostgreSQL 18 and the database supervisor
mise run dev:all                    # eagerly start every available database
mise run dev -- -D                  # ... detached; then `mise run dev:wait`, and
                                    # `mise run dev:down` when finished. Agents must do this.

./test.sh                           # Full suite against PostgreSQL 18 (the default target)
./test.sh pg16                      # Against PostgreSQL 16
./test.sh crdb                      # Against CockroachDB
./test.sh all                       # Every target (pg14-18 + crdb)
./test.sh pg16 -run TestConnect     # Trailing arguments are passed to `go test`

go test ./...                       # Also works: mise loads the default target's PGX_TEST_*
go test -race ./...                 # With the race detector

goimports -w .                      # Format (always run after making changes)
golangci-lint run ./...             # Lint

mise run dev:ports                  # This checkout's ports and where each server's data lives
mise run db:start pg16 crdb         # Prewarm targets; tests then leave them running
mise run db:stop pg16 crdb          # Stop prewarmed targets
mise run db:psql                    # psql against PostgreSQL 18; `mise run db:psql 16` for another
process-compose process logs pg16   # One server's output
```

Do not hardcode database ports. They are allocated per checkout by port-tamer and read from the
environment (`PGPORT`, `PGPORT_16`, `CRDB_PORT`) or `.dev/ports.env`; 5432 and 26257 mean nothing
here.

The `PGX_TEST_*` connection strings have one definition, `scripts/lib/test_targets.rb`. Add or
change a target there, never in a second copy.

## Test Database Setup

The lifecycle scripts handle setup: a PostgreSQL server initializes its cluster on first start and
creates `pgx_test` with the extensions and auth roles from `testsetup/postgresql_setup.sql`.
CockroachDB recreates `pgx_test` after each in-memory restart. Nothing needs to be set up by hand.

Contributors who would rather point pgx at a PostgreSQL server they already have can set
`PGX_TEST_DATABASE` themselves; see CONTRIBUTING.md. Many tests are skipped unless additional
`PGX_TEST_*` variables are set (for TLS, SCRAM, MD5, unix socket, PgBouncer testing).

## Reference Material

`references/` holds read-only reference checkouts used when building pgx — currently the PostgreSQL source tree pinned to `REL_18_STABLE`. It is gitignored and provisioned on demand: bare mirrors are cached at a machine-level path (`/persist/shared/references` in a devcontainer, `~/.local/share/pgx/references` natively; `REFERENCES_MIRROR_DIR` overrides) and lightweight local checkouts are created in `references/` with `rake references:setup`. Each checkout has per-instance Git metadata while borrowing the shared mirror's object store. Related tasks: `rake references:update`, `rake references:status`, `rake references:clean`.

- Do not automatically provision or update `references/`.
- Never run `rake references:setup`, `rake references:update`, or any large download on your own initiative.
- If reference sources are missing, work without them or ask the user.

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
- **Formatting** — always run `goimports -w .` after making changes to ensure code is properly formatted. CI checks formatting via `gofmt -l -s -w . && git diff --exit-code`. `gofumpt` with extra rules is also enforced via `golangci-lint`.
- **Linters** — `govet`, `ineffassign`, and `unconvert` only (configured in `.golangci.yml`).
- **CI matrix** — tests run against Go 1.25/1.26 × PostgreSQL 14-18 + CockroachDB, on Linux and Windows. Race detector enabled on Linux only.
