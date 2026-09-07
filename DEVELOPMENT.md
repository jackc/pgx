# Developing pgx

Two supported environments, one command set. Native macOS or Linux is the fast inner loop; the
devcontainer is a Linux compatibility and isolation option. Both run the same mise tasks against
the same `process-compose.yaml`, so nothing here is specific to one of them except the
prerequisites in §1.

```sh
scripts/setup-host # native macOS or Ubuntu host prerequisites (once per machine)
export PATH="$HOME/.local/bin:$PATH" # if mise was just installed
mise trust          # trust this checkout's configuration
mise install        # tool versions from mise.toml
mise run dev:init   # this checkout's ports and certificates
mise run dev        # start PostgreSQL 18 and the on-demand database supervisor
./test.sh           # the suite against PostgreSQL 18
./test.sh all       # every target; non-default servers start and stop around their tests
```

---

## 1. Prerequisites

### Native

[mise](https://mise.jdx.dev) provides Go, Ruby, CockroachDB, process-compose, and port-tamer.
PostgreSQL itself is the one thing it does not: pgx tests against five major versions, and they
come from the system package manager.

```sh
scripts/setup-host
```

On macOS, install [Homebrew](https://brew.sh) first. The script uses `Brewfile` to install
PostgreSQL 14-18 and Ruby build dependencies. On Ubuntu, it uses sudo when needed, configures
the official PostgreSQL apt repository, and installs the same server versions and build
dependencies. Other Linux distributions require manual prerequisite installation.

The Ubuntu installer temporarily disables automatic `main` cluster creation during server
installation and removes that override on exit, leaving the package-owned cluster configuration
unchanged. The macOS installer does not start Homebrew services.

The dispatcher installs mise if it is missing and prints its executable path. It does not run
`mise trust`, install project tools, allocate ports, initialize databases, or start servers. Run
the remaining commands above explicitly as your regular development user.

- **Do not** `brew services start` any of them, and on Debian do not let the packages create a
  machine-wide cluster. pgx runs its own clusters per checkout.
- The Homebrew formulas are *keg-only*, so `psql` and `pg_isready` are not on your `PATH` after
  install. `mise.toml` adds PostgreSQL 18's `bin` to `PATH` for this project — one client serves
  every server, since libpq is backward compatible.
- Only the majors you actually test against need to be installed. `mise run dev` looks for each
  one's server binaries and names any that are missing; `./test.sh pg15` reports a direct install
  hint rather than trying to launch a missing server. `PGBIN_16` and friends override the search
  for a major built or installed somewhere unusual.
- Clusters are created with the `en_US.UTF-8` locale where the system has it (matching CI and the
  container images this replaced), falling back to `C.UTF-8` and then `C`. A stock Debian or Ubuntu
  has only `C.UTF-8` unless you have run `locale-gen en_US.UTF-8`.

### Devcontainer

Reopen in the container; `.devcontainer/` handles the rest. It installs the same five PostgreSQL
servers and runs the same per-checkout clusters — it is a Linux shell around this same setup, not a
second architecture.

---

## 2. Checkouts are the unit of isolation

A git worktree is the native equivalent of a second devcontainer instance. Each one gets:

```
.dev/                          # gitignored, per-checkout runtime state
  ports.env                    # this checkout's TCP ports (port-tamer's state file)
  derived.env                  # PG* defaults and the default target's PGX_TEST_* set
  certs/                       # the client certificates the TLS tests use
  logs/                        # one log per service
  <os>-<arch>/postgres/run     # one socket directory, shared by all five servers (mode 0700)
  <os>-<arch>/postgres/<major>/data
  <os>-<arch>/crdb/
```

The last three — the clusters, the CockroachDB store and the socket directory — move when
`PGX_DEV_RUNTIME_DIR` is set. The devcontainer sets it to `/persist/local/dev`, on its own named
volume: `.dev/` is inside the `/workspaces/pgx` bind mount, and `initdb` and Unix sockets do not
work reliably on Docker Desktop's macOS/Windows file sharing. (The per-version PostgreSQL
containers this setup replaced used named volumes for their data and socket directories for the
same reason.) Everything else stays in the checkout.

```sh
git worktree add ../pgx-feature-x feature-x
cd ../pgx-feature-x
mise run dev:init && mise run dev
```

Both checkouts run simultaneously: different ports, independent database state, independent
process-compose instances. `mise run dev:ports` prints the allocation.

Ports are allocated once per checkout by [port-tamer](https://github.com/jackc/port-tamer) and then
persisted. `port-tamer.toml` declares which ports a checkout needs — **append new entries at the
end**, since inserting or reordering renumbers the existing ones. A listening port never moves an
existing allocation, because it may well belong to this checkout's own running services; when two
checkouts genuinely collide, stop one and run `mise run dev:ports:overwrite`.

The data directories are keyed by platform because a cluster `initdb`'d by the Linux devcontainer
cannot be read by a native macOS server, and one checkout may be opened both ways. After switching
a checkout between the two, re-run `mise run dev:ports:ensure` so the derived paths follow.

**Reference sources** (`references/`) are provisioned per checkout with `rake references:setup`,
sharing one machine-level mirror (`/persist/shared` in a container, `~/.local/share/pgx` natively;
`REFERENCES_MIRROR_DIR` overrides). They are a multi-GB download; nothing provisions them
automatically.

---

## 3. The databases

`mise run dev` starts PostgreSQL 18 under
[process-compose](https://github.com/F1bonacc1/process-compose). PostgreSQL 14-17 and a single-node
in-memory CockroachDB are registered with the same supervisor but disabled initially, ready to be
started by a test or an explicit `db:start`. `mise run dev:all` eagerly starts every available
server. These are the same services the devcontainer used to run as containers; they are now
ordinary processes against this checkout's own clusters.

Each PostgreSQL server initializes its cluster on first start — `initdb`, this project's
`pg_hba.conf`, and the TLS certificates — then creates `pgx_test` with its extensions and its
`pgx_md5` / `pgx_scram` / `pgx_pw` / `pgx_ssl` / `pgx_sslcert` roles. The default server uses its
follow-on `pg18-setup` process; the lazy lifecycle performs the same idempotent setup directly for
an on-demand target. Restarts are free once the cluster exists. If setup fails partway it drops the
half-built database so the next start retries rather than reporting it as present.

CockroachDB has no separate setup process: its store is in memory, so every restart is an empty
cluster and its readiness probe creates `pgx_test` itself.

```sh
process-compose process list             # status, scriptable
process-compose process logs pg16        # one server's output
mise run db:start pg16 crdb               # prewarm one or more targets
mise run db:stop pg16 crdb                # stop them again
mise run db:start all                     # prewarm every target
process-compose down                     # stop this checkout's stack only
mise run db:psql                         # psql against the already-running PostgreSQL 18
mise run db:start pg16                   # prewarm PostgreSQL 16 before interactive use
mise run db:psql 16                      # psql against PostgreSQL 16
mise run db:psql 16 -c 'select 1'        # arguments after the major go to psql
```

The `process-compose` commands need no flags: `PC_PORT_NUM` is part of this checkout's
environment, so they never reach another checkout's stack.

`rake db:psql[16]` is the same as `mise run db:psql 16`. Note the brackets — a bare
`rake db:psql 16` is rake asking for a *task* named `16`, not an argument, so the `db:*` tasks
reject it rather than quietly acting on every cluster.

`mise run db:reset` destroys and re-creates clusters and refuses to run while the selected servers
are up: removing a data directory under a live postmaster corrupts it. Stop selected targets with
`mise run db:stop`, or stop the whole supervisor with `mise run dev:down`.

All five PostgreSQL servers share **one** Unix socket directory. Sockets are named
`.s.PGSQL.<port>`, so the port picks the server — which is what lets a single
`PGX_TEST_UNIX_SOCKET_CONN_STRING` work for every major, exactly as the container's shared
`/var/run/postgresql` volume did.

Tests preserve explicit choices: if `db:start` prewarmed a target, tests leave it running. If a
test had to start the target, it stops it in an `ensure` block whether the suite passes, fails, or
is interrupted. A per-target lock prevents concurrent test commands from stopping a server the
other is using.

---

## 4. Running tests

```sh
./test.sh                        # PostgreSQL 18, the default target
./test.sh pg14                   # PostgreSQL 14
./test.sh crdb                   # CockroachDB
./test.sh all                    # every target, sequentially
./test.sh pg16 -run TestConnect  # trailing arguments go to `go test`
```

`mise run test` and `mise run test:all` are equivalent. All of them require the stack to be
running, but only their selected database target needs to be up. A stopped target is started,
bootstrapped, and stopped automatically. `./test.sh all` does this sequentially, so at most the
default PostgreSQL 18 plus one additional database is normally resident.

A bare `go test ./...` also works: mise loads `.dev/derived.env`, which carries the default
target's full `PGX_TEST_*` set, so an activated shell is already pointed at PostgreSQL 18.

Those connection strings have exactly one definition, `scripts/lib/test_targets.rb`. Both consumers
— the generated `.dev/derived.env` and `./test.sh <target>` — read it, so there is no second copy
to drift.

Some tests only run when their environment variable is set. `go test ./... -v | grep SKIP` shows
what is being skipped; on a healthy stack that is the PgBouncer, OAuth, CrateDB, and libpq-oracle
tests, which are CI-only or manual.

---

## 5. Everyday commands

| Command | What it does |
|---|---|
| `mise run dev` | start PostgreSQL 18 and the on-demand database supervisor |
| `mise run dev:all` | eagerly start every available database |
| `mise run dev -- -D` | start the default stack detached, for CI and agents |
| `mise run dev:wait` / `dev:down` | wait for a detached stack / stop it |
| `mise run dev:init` | allocate this checkout's ports, decode its certificates |
| `mise run dev:ports` | the allocation, and where each server's data lives |
| `mise run test [target]` | the suite against one target |
| `mise run test:all` | every target |
| `mise run db:start [targets...]` / `db:stop` | prewarm or stop targets (`pg16`, `crdb`, or `all`) |
| `mise run db:init [major]` / `db:psql [major]` / `db:reset [major]` | create / open / rebuild the clusters |
| `mise run fmt` | `goimports -w .` |
| `mise run generate` | regenerate the ERB-templated sources |

`rake <task>` remains equally valid and is where some of the logic lives; `rake -T` lists
everything, including tasks with no mise wrapper (`references:*`, `db:setup`).

---

## 6. Notes

- **`PGHOST` and `PGPORT` are a pair.** They come from `.dev/` via mise — `PGPORT` from the
  allocation, `PGHOST` derived from it. Setting one without the other names a real port on the
  wrong server, and the error will point at a socket nothing ever created. `mise run dev` asserts
  the two agree with the cluster before starting anything.
- **`.dev/derived.env` is generated.** If it is ever hand-edited into something mise's dotenv
  parser rejects, every `mise` command in the directory fails — including the one that would
  rewrite it. Recover with `rm .dev/derived.env && mise run dev:ports:ensure`.
- **A `~/.psqlrc` that changes session defaults** can break the database bootstrap while leaving
  interactive use fine. The scripts pass `--no-psqlrc` for that reason.
- **`./test.sh` needs Ruby 3.0+**, which `mise install` provides. If mise is installed but not yet
  activated in your shell, `./test.sh` re-runs itself through `mise exec` rather than failing on
  the system interpreter.
- **CI does not use any of this.** `.github/workflows/ci.yml` installs system PostgreSQL through
  `ci/setup_test.bash` and carries its own copy of the connection strings. Local runs and CI
  exercise the same auth paths — `PGX_TEST_DATABASE` connects as `pgx_md5`, and one
  `testsetup/pg_hba.conf` serves both — but the two setups are still independent.
- **PgBouncer and the PG18 OAuth validator module** are exercised only in CI. Neither is part of
  the local stack, and both were absent from the devcontainer too.
