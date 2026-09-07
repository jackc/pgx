#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/dev_ready.rb — require this checkout's development stack to be running.
#
#   dev_ready.rb            fail immediately if the stack is not up and ready
#   dev_ready.rb --wait     block until it is (for a detached stack: CI, agents)
#
# A PRECONDITION only: it never starts the supervisor or any service. `mise run dev` owns the
# supervisor and PostgreSQL 18; scripts/runtests.rb may start a selected non-default target once
# that supervisor exists. A bare `go test` therefore never leaves background processes behind.

require "rbconfig"
require_relative "lib/dev_paths"
require_relative "lib/dev_services"
require_relative "lib/test_targets"

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

# `project is-ready` cannot represent the on-demand lifecycle. A disabled process becomes
# Completed (with health Unknown) after it is started and deliberately stopped, and the project
# command then treats it as permanently unready. Read the process state instead. The default
# PostgreSQL is required unless this machine does not have it installed (Disabled); other servers
# are required only when this invocation finds them active, as with `dev:all`.
def required_servers(states)
  states.filter_map do |state|
    name = state.fetch("name")
    next unless TestTargets.valid?(name)

    status = state.fetch("status")
    next if status == "Disabled"
    next if name != TestTargets.default && status == "Completed"

    name
  end
end

def servers_ready?(states, required)
  required.all? do |name|
    state = states.find { |process| process.fetch("name") == name }
    state && state.fetch("is_running") && state.fetch("is_ready") == "Ready"
  end
end

def await_servers(wait)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BOOTSTRAP_TIMEOUT
  states = DevServices.states
  required = required_servers(states)

  loop do
    return true if servers_ready?(states, required)
    return false unless wait && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

    sleep 0.5
    states = DevServices.states
  end
end

# Process readiness does not cover the database bootstrap: the pgN-setup processes are one-shots,
# and process-compose stops probing a process once it completes — so giving them a probe would not
# work either. Without this second phase,
# `mise run dev -- -D && mise run dev:wait` returns while pgx_test is still being created and the
# `go test` that follows fails on `database "pgx_test" does not exist`.
#
# Only servers that are actually ANSWERING are waited for. An on-demand or unavailable target is
# disabled by scripts/dev.rb and must not hold this up.
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

# DevServices.states invokes the process-compose client, which reads PC_PORT_NUM and PC_ADDRESS
# from the environment and therefore reaches this checkout's supervisor without explicit flags.
begin
  abort NOT_READY unless await_servers(wait)
rescue DevServices::Error => e
  abort(e.message.start_with?("process-compose is not installed") ? "dev: #{e.message}" : NOT_READY)
end

abort BOOTSTRAP_FAILED unless await_bootstrap(wait)
