#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/devcrdb.rb — this checkout's CockroachDB node, the `crdb` test target.
#
#   devcrdb.rb serve      exec the foreground node                  (what process-compose supervises)
#   devcrdb.rb ready      create pgx_test if absent, then probe it   (the readiness probe)
#   devcrdb.rb testready  is pgx_test usable?                        (scripts/runtests.rb)
#   devcrdb.rb setup      create the pgx_test database               (manual / rake)
#   devcrdb.rb sql        an interactive SQL shell against it
#
# Mirrors scripts/devdb.rb in shape so process-compose.yaml holds no ports or paths. cockroach is
# pinned in mise.toml, replacing the cockroachdb/cockroach container image the devcontainer ran.
#
# The node is insecure, single, and in-memory — exactly the devcontainer's configuration. Nothing
# persists across a restart, and the only state that matters is the pgx_test database; the pgx
# tests build everything else they need.
#
# WHY `ready` creates the database rather than a one-shot `crdb-setup` process doing it. The store
# being in memory means every restart of the node is a fresh, empty cluster — but process-compose
# does not re-run a completed one-shot when its dependency restarts. So a crash-restart, or a
# manual `process-compose process restart crdb`, left the node reporting Ready with no pgx_test in
# it, and `./test.sh crdb` failed every single test on `database "pgx_test" does not exist`. Making
# the probe itself responsible for the database is what keeps "ready" meaning what the tests need
# it to mean, on the first start and on every restart alike. The old test.sh recreated the database
# on every invocation for exactly the same reason.

require "fileutils"
require "open3"
require_relative "lib/dev_paths"

def port = DevPaths.port("CRDB_PORT")
def http_port = DevPaths.port("CRDB_HTTP_PORT")

# --insecure means no authentication and no TLS, so the URL needs sslmode=disable. root is the
# bootstrap superuser cockroach creates.
def url(database = "") = "postgresql://root@127.0.0.1:#{port}/#{database}?sslmode=disable"

def cockroach_missing
  <<~MSG
    devcrdb: cockroach not found. It is a project tool, pinned in mise.toml:
      mise install
  MSG
end

def run(*args, **opts)
  ok = system("cockroach", *args, **opts)
  abort cockroach_missing if ok.nil?
  ok
end

def sql(*args, **opts) = run("sql", "--insecure", "--host=127.0.0.1:#{port}", *args, **opts)

# Ask the catalogue, not the connection. `cockroach sql --database=pgx_test -e 'select 1'` looks
# like the obvious probe and is useless: CockroachDB accepts a session database that does not
# exist and the query still returns 1, so the check would pass against an empty node — the exact
# failure this is here to catch. A count over `show databases` is the honest question, and it fails
# outright when the node is down.
def usable?
  out, status = Open3.capture2("cockroach", "sql", "--insecure", "--host=127.0.0.1:#{port}",
                               "--format=csv", "-e",
                               "select count(*) from [show databases] where database_name = 'pgx_test'",
                               err: File::NULL)
  status.success? && out.lines.last.to_s.strip == "1"
rescue Errno::ENOENT
  abort cockroach_missing
end

def create_database(quiet: false)
  opts = quiet ? { out: File::NULL, err: File::NULL } : { out: File::NULL }
  sql("-e", "create database if not exists pgx_test", **opts)
end

case ARGV[0]
when "serve"
  # cockroach writes logs and heap/goroutine profiles relative to its FIRST store directory, and an
  # in-memory store has no directory — so it falls back to the process's working directory, which
  # is the repository root. In a container that was invisible; natively it drops memprof.*.pprof
  # and memmonitoring.* files into the checkout. Give it a home under .dev and run it from there.
  store = DevPaths.crdb_store
  FileUtils.mkdir_p(File.join(store, "logs"))
  Dir.chdir(store)

  # --store=type=mem keeps the data entirely in RAM (the container used the same 1GiB), so a
  # restart is always a clean slate.
  #
  # --http-addr is pinned to this checkout's allocation rather than left at cockroach's default
  # 8080: no test uses the admin UI, but the node binds it unconditionally and two checkouts would
  # otherwise collide on it.
  exec("cockroach", "start-single-node", "--insecure",
       "--listen-addr=127.0.0.1:#{port}",
       "--http-addr=127.0.0.1:#{http_port}",
       "--store=type=mem,size=1024MiB",
       "--log-dir=#{File.join(store, 'logs')}")
when "ready"
  # The common case is one round trip: the database is already there. Creating it is idempotent, so
  # the recheck afterwards is what distinguishes "node was down" from "node came back empty".
  exit(usable? || (create_database(quiet: true) && usable?) ? 0 : 1)
when "testready"
  exit(usable? ? 0 : 1)
when "setup"
  abort "devcrdb: could not create pgx_test" unless create_database
  puts "  crdb: pgx_test ready on 127.0.0.1:#{port}"
when "info"
  puts "127.0.0.1\t#{port}"
when "url"
  puts url("pgx_test")
when "sql"
  exec("cockroach", "sql", "--insecure", "--host=127.0.0.1:#{port}", "--database=pgx_test", *ARGV[1..])
else
  abort("usage: devcrdb.rb serve|ready|testready|setup|info|url|sql")
end
