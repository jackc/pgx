#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/devdb.rb — this checkout's own PostgreSQL clusters, one per tested major.
#
#   rake db:init          idempotent initdb of every cluster under .dev
#   rake db:init[16]      just PostgreSQL 16
#   rake db:psql[16]      psql against one cluster
#   rake db:reset[16]     destroy and re-init (destructive; requires CONFIRM=yes)
#
#   devdb.rb serve 16     init if needed, then exec the foreground server (process-compose runs this)
#   devdb.rb ready 16     exec pg_isready against it        (process-compose's readiness probe)
#   devdb.rb setup 16     create pgx_test + its roles       (process-compose, after healthy)
#   devdb.rb testready 16 is pgx_test actually USABLE yet?  (scripts/runtests.rb, dev_ready.rb)
#   devdb.rb info 16      print "<socket dir>\t<port>"      (so callers report the REAL endpoint)
#
# `ready` and `testready` are deliberately different questions. `ready` is "the postmaster accepts
# connections", which is all process-compose can gate the bootstrap on — the bootstrap is what
# creates pgx_test, so it cannot require pgx_test. `testready` is "the bootstrap has finished and
# this server can run the suite", which is what a test run actually needs; asking the first when
# you mean the second is how `./test.sh` used to race the bootstrap on a fresh checkout and
# produce a wall of `database "pgx_test" does not exist`.
#
# `serve`, `ready` and `info` exist so process-compose.yaml contains no resolved paths or ports:
# the binary location, the data directory and the socket directory are computed HERE, once, and
# the supervisor, its probe and its reporter agree by construction. exec (not spawn) keeps
# process-compose supervising the real server.
#
# WHY a cluster per checkout per major rather than one shared server: destructive testing and
# resets stay local, two checkouts can run at once, and the layout matches the devcontainer model
# it replaces — one container per major, its own data volume. All five clusters share ONE Unix
# socket directory, exactly as the container's pg-sockets volume was shared, because
# PGX_TEST_UNIX_SOCKET_CONN_STRING names a directory and finds the server by port.
#
# The servers run in the FOREGROUND under process-compose, not as pg_ctl daemons: one supervisor
# owns start/stop/restart/logs for everything, and a readiness probe gates the database bootstrap
# instead of a sleep.

require "fileutils"
require "open3"
require "shellwords"
require_relative "lib/dev_paths"
require_relative "lib/pg_bin"

TESTSETUP = File.join(DevPaths::ROOT, "testsetup")

# The base64-encoded server material, checked in under testsetup/certs, and where each piece has
# to land inside PGDATA for testsetup/postgresql_ssl.conf to find it. The container did this in
# testsetup/pg_ssl_init.sh, a docker-entrypoint-initdb.d hook; this is its portable equivalent.
SERVER_CERTS = {
  "server.crt" => "localhost.crt.b64",
  "server.key" => "localhost.key.b64",
  "root.crt" => "ca.pem.b64",
}.freeze

def majors(argv)
  return DevPaths::PG_MAJORS if argv.empty?

  argv.map do |arg|
    major = Integer(arg, exception: false)
    unless DevPaths::PG_MAJORS.include?(major)
      abort "devdb: unknown PostgreSQL major #{arg.inspect} (have: #{DevPaths::PG_MAJORS.join(', ')})"
    end
    major
  end
end

# Subcommands that address ONE cluster take an optional leading major and, for `psql`, pass
# everything after it through. Splitting on "does the first argument look like a version number"
# is what makes both `devdb.rb psql 16 -c 'select 1'` and `devdb.rb psql -c 'select 1'` work; the
# previous version ran every argument through `majors` and so rejected the first psql flag,
# leaving its own pass-through unreachable.
def split_major(argv)
  return [DevPaths::DEFAULT_PG_MAJOR, argv] unless Integer(argv.first.to_s, exception: false)

  [majors([argv.first]).first, argv[1..]]
end

# For subcommands with nothing to pass through: an optional major and nothing else.
def one_major(argv)
  major, rest = split_major(argv)
  abort "devdb: unexpected argument #{rest.first.inspect}" unless rest.empty?

  major
end

def initialized?(major) = File.exist?(File.join(DevPaths.pgdata(major), "PG_VERSION"))

# initdb's --locale must name a locale the OS actually has, and there is no portable one that is
# both UTF-8 and universally present:
#
#   * en_US.UTF-8 is what CONTRIBUTING.md's manual procedure and the postgres container images
#     have always used, so it is preferred — collation-sensitive expectations do not move.
#   * A stock Debian or Ubuntu generates only C.UTF-8. en_US.UTF-8 exists there only after a
#     locale-gen, which DEVELOPMENT.md's native-Linux instructions do not ask for — so `initdb
#     --locale=en_US.UTF-8` simply failed, five restarts in a row, on the path we document.
#   * C is the last resort and always exists.
#
# `locale -a` spells the same locale differently per platform (en_US.utf8 on glibc, en_US.UTF-8 on
# macOS), hence the normalization.
LOCALE_PREFERENCES = ["en_US.UTF-8", "C.UTF-8", "C"].freeze

def available_locales
  out = `locale -a 2>/dev/null`
  $?&.success? ? out.split("\n").map(&:strip).reject(&:empty?) : []
rescue SystemCallError
  []
end

def initdb_locale
  have = available_locales.map { |l| l.downcase.delete("-_") }
  LOCALE_PREFERENCES.find { |l| have.include?(l.downcase.delete("-_")) } || "C"
end

# --- init -----------------------------------------------------------------------------------------

def init(major)
  pgdata = DevPaths.pgdata(major)

  if initialized?(major)
    puts "  pg#{major}: cluster exists at #{pgdata}"
  else
    initdb = PgBin.tool("initdb", major)
    FileUtils.mkdir_p(File.dirname(pgdata))
    puts "  pg#{major}: initdb #{pgdata}"

    locale = initdb_locale
    puts "  pg#{major}: locale #{locale}" unless locale == LOCALE_PREFERENCES.first
    ok = system(initdb, "-D", pgdata, "-U", "postgres", "--encoding=UTF8", "--locale=#{locale}",
                "--no-sync", out: File::NULL)
    abort "devdb: pg#{major}: initdb failed" unless ok

    configure(major)
  end

  DevPaths.ensure_socket_dir!
end

# Everything the container did through mounted files and `-c` flags. Written into the cluster once,
# at creation, EXCEPT the port: that stays a command-line argument in server_argv, so re-allocating
# this checkout's ports never invalidates an existing cluster.
def configure(major)
  pgdata = DevPaths.pgdata(major)

  # listen_addresses matches CI's setting exactly. testsetup/pg_hba.conf grants the md5/scram/
  # password/cert roles 127.0.0.1 only, so listening more widely would just be unreachable surface.
  ssl_conf = File.read(File.join(TESTSETUP, "postgresql_ssl.conf"))
  File.open(File.join(pgdata, "postgresql.conf"), "a") do |f|
    f.puts
    f.puts "# Appended by scripts/devdb.rb. The port is passed on the command line instead, so"
    f.puts "# re-allocating this checkout's ports does not invalidate the cluster."
    f.puts "listen_addresses = '127.0.0.1'"
    f.puts ssl_conf
  end

  # One pg_hba.conf for local development and CI alike. The devcontainer needed a second variant
  # only because it connected as `postgres` over TCP; PGX_TEST_DATABASE now uses pgx_md5, as CI does.
  FileUtils.cp(File.join(TESTSETUP, "pg_hba.conf"), File.join(pgdata, "pg_hba.conf"))

  SERVER_CERTS.each do |name, source|
    File.binwrite(File.join(pgdata, name), File.read(File.join(DevPaths::TESTSETUP_CERTS, source)).unpack1("m"))
  end
  # PostgreSQL refuses to start if its private key is group- or world-readable.
  File.chmod(0o600, File.join(pgdata, "server.key"))
end

# --- serve / probe --------------------------------------------------------------------------------

def server_argv(major)
  [PgBin.tool("postgres", major),
   "-D", DevPaths.pgdata(major),
   "-p", DevPaths.pgport(major),
   "-k", DevPaths.socket_dir,
   "-h", "127.0.0.1"]
end

def serve(major)
  init(major) unless initialized?(major)
  DevPaths.ensure_socket_dir! # a /tmp fallback dir can vanish between reboots
  exec(*server_argv(major))
end

# --- setup ----------------------------------------------------------------------------------------

def psql_common(major) = ["-h", DevPaths.socket_dir, "-p", DevPaths.pgport(major), "-U", "postgres"]

# Run one scalar query and return its trimmed output, or nil if psql could not run it at all.
# --no-psqlrc is not optional: a developer's ~/.psqlrc can set session defaults (a read-only
# default transaction, say) that break a scripted run while leaving interactive use fine.
def query(major, database, sql)
  out, status = Open3.capture2(*(["psql"] + psql_common(major) +
                                 ["-d", database, "--no-psqlrc", "-tAc", sql]), err: File::NULL)
  status.success? ? out.strip : nil
end

# testsetup/postgresql_setup.sql is NOT idempotent — it creates extensions and roles unconditionally
# — so this runs it only when the database is absent, exactly as the container's
# docker-entrypoint-initdb.d hook only ran on a fresh volume. That makes restarting the stack free.
def setup(major)
  socket = DevPaths.socket_dir
  port = DevPaths.pgport(major)
  common = psql_common(major)

  exists = query(major, "postgres", "select 1 from pg_database where datname = 'pgx_test'")
  if exists.nil?
    abort "devdb: pg#{major}: cannot reach the server on #{socket} port #{port} to set it up."
  end

  if exists == "1"
    puts "  pg#{major}: pgx_test already present"
    return
  end

  puts "  pg#{major}: creating pgx_test"
  system("createdb", *common, "pgx_test") or abort("devdb: pg#{major}: createdb failed")

  ok = system("psql", *common, "-d", "pgx_test", "--no-psqlrc", "--quiet",
              "-v", "ON_ERROR_STOP=1", "-f", File.join(TESTSETUP, "postgresql_setup.sql"))

  unless ok
    # Undo the createdb. Every later run gates on the database EXISTING, so a pgx_test left behind
    # half-built would be reported as "already present" for the life of the cluster while tests
    # failed on a missing hstore or a missing pgx_md5 role — and the only documented recovery,
    # db:reset, destroys the whole cluster. Dropping it here makes the next start retry instead.
    system("dropdb", *common, "--if-exists", "pgx_test", out: File::NULL, err: File::NULL)
    abort "devdb: pg#{major}: postgresql_setup.sql failed (pgx_test dropped; it will be retried)"
  end
end

# --- test readiness ---------------------------------------------------------------------------------

# Not just "does pgx_test exist" — the roles and extensions are what the suite actually needs, and
# they are created by a separate, non-idempotent step that can fail after the database is created.
# Checking one of each turns a half-built database into a clear failure instead of hundreds of
# confusing test errors.
def test_ready?(major)
  query(major, "pgx_test", <<~SQL) == "1"
    select 1
    where exists (select 1 from pg_extension where extname = 'hstore')
      and exists (select 1 from pg_roles where rolname = 'pgx_md5')
  SQL
end

# Is a postmaster answering for this major right now? Used to refuse a destructive reset under a
# running server, where `rm -rf` on PGDATA corrupts the cluster and strands the socket lock file.
def running?(major)
  system("pg_isready", "--quiet", "-h", DevPaths.socket_dir, "-p", DevPaths.pgport(major),
         "-U", "postgres", out: File::NULL, err: File::NULL)
end

# --- entry point ------------------------------------------------------------------------------------

case ARGV[0]
when "init"
  majors(ARGV[1..]).each { |m| init(m) }
  puts "  socket: #{DevPaths.socket_dir}"
when "serve"
  abort "devdb: serve takes exactly one major" unless ARGV.length == 2
  serve(one_major(ARGV[1..]))
when "ready"
  major = one_major(ARGV[1..])
  exec("pg_isready", "--quiet", "-h", DevPaths.socket_dir, "-p", DevPaths.pgport(major), "-U", "postgres")
when "setup"
  majors(ARGV[1..]).each { |m| setup(m) }
when "testready"
  exit(test_ready?(one_major(ARGV[1..])) ? 0 : 1)
when "info"
  major = one_major(ARGV[1..])
  puts "#{DevPaths.socket_dir}\t#{DevPaths.pgport(major)}"
when "psql"
  major, args = split_major(ARGV[1..])
  abort "devdb: pg#{major}: cluster not initialized — run `mise run db:init`" unless initialized?(major)
  exec("psql", "-h", DevPaths.socket_dir, "-p", DevPaths.pgport(major), "-U", "postgres",
       "-d", "pgx_test", *args)
when "reset"
  unless ENV["CONFIRM"] == "yes"
    abort "devdb: `db:reset` DESTROYS the cluster data under .dev. Re-run with CONFIRM=yes if that is what you want."
  end

  targets = majors(ARGV[1..])

  # Check them ALL before destroying any. Removing a PGDATA out from under its own postmaster does
  # not stop the server: it keeps writing to unlinked inodes, its socket lock file still holds the
  # shared socket directory so the restarted server cannot bind, and process-compose burns its
  # restart budget on the churn. Even in the best case the re-initdb'd cluster has no pgx_test,
  # because the pgN-setup one-shot has already completed and process-compose will not re-run it.
  # Requiring the stack to be down means the next `mise run dev` bootstraps the new cluster.
  live = targets.select { |m| running?(m) }
  unless live.empty?
    abort <<~MSG
      devdb: #{live.map { |m| "pg#{m}" }.join(', ')} #{live.one? ? 'is' : 'are'} still running on #{DevPaths.socket_dir}.
      A reset removes the data directory, which corrupts a server that is using it. Stop the stack
      first:
          mise run db:stop #{live.map { |m| "pg#{m}" }.join(' ')}
      Or stop the entire supervisor with `mise run dev:down`.
    MSG
  end

  targets.each do |major|
    puts "  pg#{major}: removing #{DevPaths.pgdata(major)}"
    FileUtils.rm_rf(File.dirname(DevPaths.pgdata(major)))
    init(major)
  end
  puts "  the next `db:start` or test run will recreate pgx_test and its roles."
else
  abort("usage: devdb.rb init|serve|ready|setup|testready|info|psql|reset [major] [args...]")
end
