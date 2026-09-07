# frozen_string_literal: true

# scripts/lib/test_targets.rb — the ONE definition of the PGX_TEST_* environment for each test
# target.
#
# This table used to exist three times: .devcontainer/docker-compose.yml (the app container's
# defaults), test.sh (per target), and .github/workflows/ci.yml (per matrix row). Three copies
# had already drifted — the container connected PGX_TEST_DATABASE as `postgres` where CI used
# `pgx_md5`, and the SCRAM / SCRAM-plus host values were swapped between compose and test.sh, so
# the container and CI were not testing the same thing.
#
# CI's values are canonical here. A local run and a CI run now exercise the same auth paths, which
# is also what lets ONE testsetup/pg_hba.conf serve both (the separate pg_hba_devcontainer.conf
# existed only to let the container connect as `postgres` over TCP).
#
# Two consumers, one source:
#   scripts/devenv.rb    writes the DEFAULT target's set into .dev/derived.env, so a bare
#                        `go test ./...` in an activated shell just works
#   scripts/runtests.rb  builds any target's set for `./test.sh pg16`
#
# Everything varies only by the checkout's allocated port and its own paths; nothing is
# hard-coded, which is what makes two checkouts able to run the suite at the same time.

require_relative "dev_paths"

module TestTargets
  module_function

  # The client TLS material, decoded per checkout by scripts/devenv.rb. The devcontainer and the
  # old test.sh wrote these to /tmp, a machine-wide namespace two checkouts silently shared.
  def ca_pem = File.join(DevPaths::CERTS_DIR, "ca.pem")
  def client_crt = File.join(DevPaths::CERTS_DIR, "pgx_sslcert.crt")
  def client_key = File.join(DevPaths::CERTS_DIR, "pgx_sslcert.key")

  # "pg14".."pg18", "crdb" — the order ./test.sh all runs them in.
  def names = DevPaths::PG_MAJORS.map { |m| "pg#{m}" } + ["crdb"]

  def default = "pg#{DevPaths::DEFAULT_PG_MAJOR}"

  def major_for(target) = target =~ /\Apg(\d+)\z/ && DevPaths::PG_MAJORS.find { |m| m.to_s == $1 }

  def valid?(target) = target == "crdb" || !major_for(target).nil?

  def label(target)
    return "CockroachDB" if target == "crdb"

    "PostgreSQL #{major_for(target)}"
  end

  # The full PGX_TEST_* environment for one target, as a Hash of String => String.
  def env_for(target)
    abort "pgx dev: unknown target #{target.inspect} (valid: #{(names + ['all']).join(', ')})" unless valid?(target)

    target == "crdb" ? crdb_env : postgres_env(major_for(target))
  end

  # Every variable name this table owns, across all targets.
  def keys = postgres_env(DevPaths::DEFAULT_PG_MAJOR).keys | crdb_env.keys

  # env_for, but with every variable this table owns that the target does NOT set mapped to nil —
  # which is how Ruby spells "remove this from the child's environment".
  #
  # This matters because Ruby's `system`/`spawn` MERGE their environment hash into the parent's
  # rather than replacing it, and mise exports the DEFAULT (PostgreSQL) target's whole set into
  # every shell in this checkout. Handing `go test` only crdb's single variable therefore left ten
  # others — the TLS, SCRAM, MD5, plain-password and unix-socket strings, and PGX_SSL_PASSWORD —
  # still pointing at PostgreSQL 18. `./test.sh crdb` ran those tests against PostgreSQL and
  # reported them as CockroachDB passes; stop pg18 and the same run failed instead.
  #
  # Anything a developer sets themselves (PGX_TEST_PGBOUNCER_CONN_STRING, say) is deliberately left
  # alone: this only clears what this file is responsible for.
  def subprocess_env_for(target)
    keys.to_h { |name| [name, nil] }.merge(env_for(target))
  end

  # CockroachDB accepts no PGX_TEST_* variable but the primary one: it has no md5/scram/cert
  # authentication to exercise and runs insecure, so every other test skips by design — which
  # depends on the others being ABSENT from the child environment, not merely unset here. See
  # subprocess_env_for.
  def crdb_env
    port = DevPaths.port("CRDB_PORT")
    {
      "PGX_TEST_DATABASE" =>
        "postgresql://root@127.0.0.1:#{port}/pgx_test?sslmode=disable&experimental_enable_temp_tables=on",
    }
  end

  def postgres_env(major)
    port = DevPaths.pgport(major)
    socket = DevPaths.socket_dir

    {
      # The primary connection every integration test uses. pgx_md5 rather than postgres: this is
      # CI's value, and testsetup/pg_hba.conf grants `postgres` only the local socket.
      "PGX_TEST_DATABASE" =>
        "host=127.0.0.1 port=#{port} user=pgx_md5 password=secret dbname=pgx_test",

      # No user: connects as the OS user, which testsetup/postgresql_setup.sql creates via its
      # `\set whoami` block. The socket directory is shared by all five majors — the port picks
      # the server.
      "PGX_TEST_UNIX_SOCKET_CONN_STRING" =>
        "host=#{socket} port=#{port} dbname=pgx_test",

      "PGX_TEST_TCP_CONN_STRING" =>
        "host=127.0.0.1 port=#{port} user=pgx_md5 password=secret dbname=pgx_test",

      "PGX_TEST_MD5_PASSWORD_CONN_STRING" =>
        "host=127.0.0.1 port=#{port} user=pgx_md5 password=secret dbname=pgx_test",

      "PGX_TEST_PLAIN_PASSWORD_CONN_STRING" =>
        "host=127.0.0.1 port=#{port} user=pgx_pw password=secret dbname=pgx_test",

      "PGX_TEST_SCRAM_PASSWORD_CONN_STRING" =>
        "host=127.0.0.1 port=#{port} user=pgx_scram password=secret dbname=pgx_test " \
        "channel_binding=disable",

      # SCRAM-PLUS and the TLS strings use `localhost`, not 127.0.0.1: sslmode=verify-full checks
      # the server certificate, whose common name is localhost (testsetup/generate_certs.go).
      "PGX_TEST_SCRAM_PLUS_CONN_STRING" =>
        "host=localhost port=#{port} user=pgx_ssl password=secret sslmode=verify-full " \
        "sslrootcert=#{ca_pem} dbname=pgx_test channel_binding=require",

      "PGX_TEST_TLS_CONN_STRING" =>
        "host=localhost port=#{port} user=pgx_ssl password=secret sslmode=verify-full " \
        "sslrootcert=#{ca_pem} dbname=pgx_test channel_binding=disable",

      "PGX_TEST_TLS_CLIENT_CONN_STRING" =>
        "host=localhost port=#{port} user=pgx_sslcert sslmode=verify-full " \
        "sslrootcert=#{ca_pem} sslcert=#{client_crt} sslkey=#{client_key} dbname=pgx_test",

      # The passphrase on the checked-in client key. Without it TestConnectTLSClientCert skips
      # rather than fails, which is why a missing value here is invisible.
      "PGX_SSL_PASSWORD" => "certpw",
    }
  end

  # The PG* defaults that make a bare `psql` — and anything else reading libpq's environment —
  # land on this checkout's default cluster over its Unix socket.
  def pg_defaults
    {
      "PGHOST" => DevPaths.socket_dir,
      "PGPORT" => DevPaths.pgport(DevPaths::DEFAULT_PG_MAJOR),
      "PGUSER" => "postgres",
      "PGDATABASE" => "pgx_test",
    }
  end
end
