#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/dev_ready.rb — require this checkout's development stack to be running.
#
#   dev_ready.rb            fail immediately if the stack is not up and ready
#   dev_ready.rb --wait     block until it is (for a detached stack: CI, agents)
#
# A PRECONDITION only: it never starts services. `mise run dev` is the single launcher, which is
# what keeps a stray `go test` from leaving five postmasters running that nothing owns. The payoff
# is the error message — "start it with mise run dev" instead of a wall of connection refusals
# from whichever test happened to connect first.

require "rbconfig"
require_relative "lib/dev_paths"

NOT_READY = <<~MSG
  The pgx development stack is not running.

  Start it in another terminal with:
      mise run dev

  Or, detached (CI and agents):
      mise run dev -- -D && mise run dev:wait
MSG

BOOTSTRAP_FAILED = <<~MSG
  The servers are up, but their pgx_test databases are not. Look at what the bootstrap said:
      process-compose process logs pg18-setup      (or crdb, pg14-setup, ...)
MSG

# See await_bootstrap below. Creating five databases takes about a second; this only has to be
# longer than that.
BOOTSTRAP_TIMEOUT = 90

def probe(script, command, *args)
  system(RbConfig.ruby, File.join(__dir__, script), command, *args, out: File::NULL, err: File::NULL)
end

TARGETS = DevPaths::PG_MAJORS.map { |m| ["devdb.rb", [m.to_s]] } + [["devcrdb.rb", []]]

# `process-compose project is-ready` reports on readiness PROBES, and the database bootstrap has
# none: the pgN-setup processes are one-shots, and process-compose stops probing a process once it
# completes — so giving them one would not work either. Without this second phase,
# `mise run dev -- -D && mise run dev:wait` returns while pgx_test is still being created and the
# `go test` that follows fails on `database "pgx_test" does not exist`.
#
# Only servers that are actually ANSWERING are waited for. A major that is not installed (disabled
# by scripts/dev.rb) or one deliberately stopped with `process-compose process stop pg14` must not
# hold this up.
def await_bootstrap(wait)
  pending = TARGETS.select { |script, args| probe(script, "ready", *args) }
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BOOTSTRAP_TIMEOUT

  loop do
    pending = pending.reject { |script, args| probe(script, "testready", *args) }
    return true if pending.empty?
    return false unless wait && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

    sleep 0.5
  end
end

wait = ARGV.include?("--wait")
unless (ARGV - ["--wait"]).empty?
  abort "usage: dev_ready.rb [--wait]"
end

abort NOT_READY unless File.exist?(DevPaths::PORTS_ENV)

# process-compose reads PC_PORT_NUM and PC_ADDRESS from the environment, so this reaches THIS
# checkout's supervisor with no flags. `project is-ready` reports on every process's readiness
# probe at once, which is exactly the condition the test tasks care about.
args = ["process-compose", "project", "is-ready"]
args << "--wait" if wait

ok = system(*args, out: File::NULL, err: File::NULL)
abort <<~MSG if ok.nil?
  dev: process-compose not found. It is a project tool, pinned in mise.toml:
    mise install
MSG

abort NOT_READY unless ok

abort BOOTSTRAP_FAILED unless await_bootstrap(wait)
