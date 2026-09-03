#!/usr/bin/env bash
# test.sh - Run the pgx test suite against a database target.
#
#   ./test.sh                        PostgreSQL 18 (default)
#   ./test.sh pg14                   PostgreSQL 14
#   ./test.sh crdb                   CockroachDB
#   ./test.sh all                    every target, sequentially
#   ./test.sh pg16 -run TestConnect  trailing arguments are passed to `go test`
#
# The targets are served by this checkout's own database clusters; start them with `mise run dev`.
# See DEVELOPMENT.md.
#
# The logic lives in scripts/runtests.rb, which builds each target's PGX_TEST_* environment from
# scripts/lib/test_targets.rb — the one place those connection strings are defined. This wrapper
# exists so `./test.sh` keeps working; `mise run test` is equivalent.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtests="$root/scripts/runtests.rb"

# Which ruby. This is the project's documented entry point, so it has to survive the state a new
# contributor is actually in after `mise install`: mise present but not yet activated in this
# shell, where a bare `ruby` is the system interpreter. macOS ships 2.6, and runtests.rb uses
# endless method definitions (3.0+) — so that path died with a wall of syntax errors naming
# nothing. Prefer a ruby new enough to run it; otherwise go through mise, which has the pinned one.
if ruby -e 'exit(RUBY_VERSION.split(".")[0].to_i >= 3 ? 0 : 1)' > /dev/null 2>&1; then
  exec ruby "$runtests" "$@"
elif command -v mise > /dev/null 2>&1; then
  exec mise exec -- ruby "$runtests" "$@"
else
  echo "test.sh: needs Ruby 3.0 or newer (mise.toml pins one)." >&2
  echo "  Install mise (https://mise.jdx.dev), then: mise install" >&2
  echo "  See DEVELOPMENT.md." >&2
  exit 1
fi
