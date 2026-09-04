#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/dev.rb — the launcher behind `mise run dev` and `mise run dev:all`.
#
# Deliberately thin, and in this exact order:
#
#   1. ensure this checkout has a port allocation (and the values derived from it);
#   2. export them into the environment;
#   3. exec process-compose.
#
# `down` is the exception and runs BEFORE any of that — see below.
#
# The ordering is the point. mise loads the environment once, before a task runs, so a task that
# CHANGES .dev/ports.env cannot have the new values reach anything through mise's own env — the
# stale-value trap. Doing the ensure and the export inside one process, immediately before exec,
# means process-compose (and therefore every server it starts, since it passes its environment
# down) sees exactly the allocation that was just validated. No per-process `environment:` blocks
# in process-compose.yaml, and one source of truth.
#
# PC_PORT_NUM is not arbitrary: it is the variable process-compose itself reads for its control
# port, so every `process-compose ...` command in this checkout targets THIS checkout's stack with
# no flags.

require "fileutils"
require "rbconfig"
require "shellwords"
require_relative "lib/dev_paths"
require_relative "lib/pg_bin"
require_relative "lib/test_targets"

# Read one of the generated dotenv files the way mise reads it, so this process and an activated
# shell end up with byte-identical values. scripts/devenv.rb double-quotes every derived value
# (the connection strings contain spaces, and mise's parser rejects those bare), so the quotes and
# their escapes have to come back off here — otherwise the assertion below compares a quoted
# PGHOST against an unquoted one and fails on a difference that is not real.
def load_env_file(path)
  return unless File.exist?(path)

  File.readlines(path).each do |line|
    next if line.start_with?("#") || !line.include?("=")

    name, value = line.chomp.split("=", 2)
    value = value[1..-2].gsub(/\\(.)/, '\\1') if value.start_with?('"') && value.end_with?('"')
    ENV[name] = value
  end
end

def require_process_compose!
  return if system("command -v process-compose > /dev/null 2>&1")

  abort <<~MSG
    dev: process-compose not found. It is a project tool, pinned in mise.toml:
      mise install
  MSG
end

# `mise run dev:down`, and anything else that only wants to REACH the running stack, comes first
# and deliberately does not re-derive the allocation.
#
# Stopping your own services must not depend on the thing that is broken. Re-running the allocation
# here meant that a saved allocation which no longer matched port-tamer.toml — the one case
# devenv.rb says a user will actually hit, whose own advice is "stop this checkout's services and
# replace it" — aborted the stop command too, leaving no way to stop them. Everything `down` needs
# (PC_PORT_NUM) is already in the environment: mise loaded it, and these files are the fallback for
# a direct `ruby scripts/dev.rb down`.
if ARGV.first == "down"
  [DevPaths::PORTS_ENV, DevPaths::DERIVED_ENV].each { |f| load_env_file(f) }
  require_process_compose!
  exec("process-compose", "down", *ARGV[1..])
end

# The ordinary development loop needs only the default PostgreSQL. `all` is consumed here rather
# than passed to process-compose as a process name; it opts into eagerly starting every available
# server while leaving all other process-compose flags untouched.
start_all = ARGV.first == "all"
ARGV.shift if start_all

system(RbConfig.ruby, File.join(__dir__, "devenv.rb"), "ensure") or
  abort("dev: port allocation failed")

# Both files mise would have loaded, in the same order (the derived values come from the
# allocation, so they are read after it).
[DevPaths::PORTS_ENV, DevPaths::DERIVED_ENV].each { |f| load_env_file(f) }

require_process_compose!

# The pair check. PGPORT comes from port-tamer's allocation and PGHOST is derived from it by
# devenv.rb, out of the same DevPaths source devdb.rb starts the servers on — but they live in
# different files, so this is where the pair is verified. Asserting rather than re-assigning keeps
# ONE source of truth and turns a drift between the scripts into a loud failure instead of a
# connection to a socket that nothing ever created.
info = `#{RbConfig.ruby.shellescape} #{File.join(__dir__, 'devdb.rb').shellescape} info`
unless $?.success?
  abort "dev: could not read the cluster layout from devdb.rb (see the error above). " \
        "If this checkout has not been bootstrapped yet, run `mise run dev:init`."
end

db_socket, db_port = info.chomp.split("\t")
if ENV["PGHOST"] != db_socket || ENV["PGPORT"] != db_port
  abort <<~MSG
    dev: the allocation disagrees with the cluster layout.
      .dev/*.env:    PGHOST=#{ENV['PGHOST'].inspect} PGPORT=#{ENV['PGPORT'].inspect}
      devdb.rb info: PGHOST=#{db_socket.inspect} PGPORT=#{db_port.inspect}
    Run `mise run dev:ports:ensure` to regenerate the derived values.
  MSG
end

# Which majors this machine can actually serve. PostgreSQL 18 is enabled by default; every other
# target is disabled until a test or `db:start` starts it manually. `dev:all` enables everything
# whose binaries exist. process-compose expands these values before parsing its config.
#
# A missing major must also be disabled: otherwise it fails, retries five times, and leaves
# `process-compose project is-ready --wait` waiting on a probe that can never pass.
skipped = DevPaths::PG_MAJORS.reject { |major| PgBin.dir(major) }
DevPaths::PG_MAJORS.each do |major|
  enabled = !skipped.include?(major) && (start_all || major == DevPaths::DEFAULT_PG_MAJOR)
  ENV["PGX_DISABLE_PG#{major}"] = (!enabled).to_s
end
ENV["PGX_DISABLE_CRDB"] = (!start_all).to_s

# process-compose.yaml names this rather than repeating .dev/logs ten times; DevPaths owns the
# layout, and process-compose expands the variable when it reads its config.
FileUtils.mkdir_p(DevPaths::LOGS_DIR)
ENV["PGX_LOGS_DIR"] = DevPaths::LOGS_DIR

puts "  socket    #{db_socket}"
DevPaths::PG_MAJORS.each do |major|
  note =
    if skipped.include?(major)
      "  (not installed — skipped)"
    elsif major == DevPaths::DEFAULT_PG_MAJOR
      "  (default target)"
    elsif !start_all
      "  (on demand)"
    else
      ""
    end
  puts format("  pg%-7s 127.0.0.1:%s%s", major, DevPaths.pgport(major), note)
end
crdb_note = start_all ? "" : "  (on demand)"
puts "  crdb      127.0.0.1:#{DevPaths.port('CRDB_PORT')}#{crdb_note}"
puts "  control   127.0.0.1:#{ENV['PC_PORT_NUM']}"
puts
unless skipped.empty?
  puts "  Not installed, so disabled for this run:"
  skipped.each { |major| puts format("    pg%-5s %s", major, PgBin.install_hint(major)) }
  puts
end
puts "  ./test.sh [pg14..pg18|crdb|all]     mise run db:start [target|all]"
puts

# exec replaces the process image WITHOUT running Ruby's at_exit or flushing its buffers. When
# stdout is a pipe rather than a TTY it is block-buffered, so everything above is silently lost —
# exactly where an agent or a CI log would need it most. Flush before handing the process over.
$stdout.flush

exec("process-compose", "--log-file", ENV.fetch("PC_SERVER_LOG_FILE", ".dev/process-compose.log"),
     "--log-no-color", "up", *ARGV)
