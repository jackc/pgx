#!/usr/bin/env bash
# test.sh - Run the pgx test suite against a database target.
#
#   ./test.sh                        PostgreSQL 18 (default)
#   ./test.sh pg14                   PostgreSQL 14
#   ./test.sh crdb                   CockroachDB
#   ./test.sh all                    every target, sequentially
#   ./test.sh pg16 -run TestConnect  trailing arguments are passed to `go test`
#
# `mise run dev` starts PostgreSQL 18 and the supervisor. Other targets start for a test and stop
# afterwards unless they were explicitly prewarmed with `mise run db:start`. See DEVELOPMENT.md.
#
# The logic lives in scripts/runtests.rb, which builds each target's PGX_TEST_* environment from
# scripts/lib/test_targets.rb — the one place those connection strings are defined. This wrapper
# exists so `./test.sh` keeps working; `mise run test` is equivalent.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtests="$root/scripts/runtests.rb"

# `mise exec` is not only how this wrapper gets the pinned Ruby. It also puts process-compose, Go,
# and CockroachDB on PATH and loads this checkout's .dev/*.env files. Prefer it whenever it is
# available, including when the shell has a new-enough system Ruby but mise has not been activated.
# Invoking it from `mise run test` is harmless: this execs Ruby directly, so there is no task
# recursion.
if command -v mise > /dev/null 2>&1; then
  exec mise exec -- ruby "$runtests" "$@"
elif ruby -e 'exit(RUBY_VERSION.split(".")[0].to_i >= 3 ? 0 : 1)' > /dev/null 2>&1; then
  exec ruby "$runtests" "$@"
else
  echo "test.sh: needs Ruby 3.0 or newer (mise.toml pins one)." >&2
  echo "  Install mise (https://mise.jdx.dev), then: mise install" >&2
  echo "  See DEVELOPMENT.md." >&2
  exit 1
fi
