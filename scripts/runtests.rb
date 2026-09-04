#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/runtests.rb — run the pgx test suite against one database target, or all of them.
# This is what ./test.sh and `mise run test` both call.
#
#   ./test.sh                       PostgreSQL 18 (the default target)
#   ./test.sh pg14                  PostgreSQL 14
#   ./test.sh crdb                  CockroachDB
#   ./test.sh all                   every target, sequentially
#   ./test.sh pg16 -run TestConnect any trailing arguments go to `go test`
#
# The PGX_TEST_* environment for each target comes from scripts/lib/test_targets.rb — the single
# definition, also used to write .dev/derived.env. PostgreSQL 18 normally stays running; another
# target is started on demand and stopped after the run unless a developer prewarmed it.

require_relative "lib/dev_services"
require_relative "lib/test_targets"

TTY = $stdout.tty?
def colour(code, text) = TTY ? "\e[#{code}m#{text}\e[0m" : text
def info(msg) = puts(colour("0;34", "==> #{msg}"))
def ok(msg) = puts(colour("0;32", "==> #{msg}"))
def err(msg) = warn(colour("0;31", "==> #{msg}"))

def run_target(target, go_args)
  label = TestTargets.label(target)
  passed = false

  DevServices.with_temporary_target(target) do
    info "Testing against #{label}"

    # subprocess_env_for, not env_for: `system` MERGES its environment hash into this process's,
    # and mise has already exported the default target's whole PGX_TEST_* set. See test_targets.rb.
    #
    # -count=1 defeats Go's test result cache: these are integration tests whose inputs (the
    # database) Go cannot see, so a cached PASS would be meaningless.
    passed = system(TestTargets.subprocess_env_for(target),
                    "go", "test", "-count=1", *go_args, "./...")
  end

  unless passed
    err "Tests FAILED against #{label}"
    return false
  end

  ok "Tests passed against #{label}"
  true
rescue DevServices::Error => e
  err "Could not prepare #{label}: #{e.message}"
  false
end

target = ARGV.first
if target.nil? || target.start_with?("-")
  go_args = ARGV
  target = TestTargets.default
else
  go_args = ARGV[1..]
end

targets =
  if target == "all"
    TestTargets.names
  else
    unless TestTargets.valid?(target)
      err "Unknown target: #{target}"
      err "Valid targets: #{(TestTargets.names + ['all']).join(', ')}"
      exit 1
    end
    [target]
  end

failed = targets.reject do |t|
  if targets.length > 1
    puts
    info "=" * 42
    info "Target: #{t}"
    info "=" * 42
  end
  run_target(t, go_args)
end

if failed.empty?
  ok "All targets passed" if targets.length > 1
  exit 0
end

puts
err "Failed targets: #{failed.join(', ')}"
exit 1
