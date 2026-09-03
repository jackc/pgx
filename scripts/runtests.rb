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
# definition, also used to write .dev/derived.env. This script's only jobs are choosing the
# target, checking the server is actually there, and reporting.

require "rbconfig"
require_relative "lib/dev_paths"
require_relative "lib/test_targets"

TTY = $stdout.tty?
def colour(code, text) = TTY ? "\e[#{code}m#{text}\e[0m" : text
def info(msg) = puts(colour("0;34", "==> #{msg}"))
def ok(msg) = puts(colour("0;32", "==> #{msg}"))
def err(msg) = warn(colour("0;31", "==> #{msg}"))

NOT_RUNNING = <<~MSG
  Start the development stack in another terminal:
      mise run dev

  Or, detached (CI and agents):
      mise run dev -- -D && mise run dev:wait
MSG

BOOTSTRAP_FAILED = <<~MSG
  Its server is answering, but pgx_test is missing or incomplete. Look at what the database
  bootstrap said:
      process-compose process logs pg18-setup      (or crdb, pg14-setup, ...)
MSG

# How long to wait for the database bootstrap. It normally takes under a second; the wait exists
# only to cover the window between a server becoming reachable and its setup step finishing.
BOOTSTRAP_TIMEOUT = 60

def probe(target, command)
  script, *args =
    if target == "crdb"
      ["devcrdb.rb", command]
    else
      ["devdb.rb", command, TestTargets.major_for(target).to_s]
    end

  system(RbConfig.ruby, File.join(__dir__, script), *args, out: File::NULL, err: File::NULL)
end

# Two different questions, and they fail differently.
#
# `ready` is "the server is answering", which is what tells a developer the stack is not running —
# without it, `go test` produces a wall of connection failures from whichever test connects first
# rather than one sentence naming the fix.
#
# `testready` is "pgx_test exists with its extensions and roles", which is what the suite actually
# needs. It is not the same instant: process-compose can only gate the database bootstrap on the
# server being reachable, so on a fresh checkout there is a window where the first question is yes
# and the second is no. A `./test.sh` landing in that window used to hand off to `go test` anyway
# and fail every test with `database "pgx_test" does not exist`. Polling closes it.
def reachable?(target) = probe(target, "ready")

def test_ready?(target) = probe(target, "testready")

def await_bootstrap(target, label)
  return true if test_ready?(target)

  info "Waiting for #{label} to finish creating pgx_test"
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BOOTSTRAP_TIMEOUT

  until test_ready?(target)
    return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

    sleep 0.5
  end

  true
end

def run_target(target, go_args)
  label = TestTargets.label(target)

  unless reachable?(target)
    err "#{label} is not reachable."
    warn NOT_RUNNING
    return false
  end

  unless await_bootstrap(target, label)
    err "#{label} is not ready to test against."
    warn BOOTSTRAP_FAILED
    return false
  end

  info "Testing against #{label}"

  # subprocess_env_for, not env_for: `system` MERGES its environment hash into this process's, and
  # mise has already exported the default target's whole PGX_TEST_* set. See test_targets.rb.
  #
  # -count=1 defeats Go's test result cache: these are integration tests whose inputs (the database)
  # Go cannot see, so a cached PASS would be meaningless.
  unless system(TestTargets.subprocess_env_for(target), "go", "test", "-count=1", *go_args, "./...")
    err "Tests FAILED against #{label}"
    return false
  end

  ok "Tests passed against #{label}"
  true
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
