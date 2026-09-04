#!/usr/bin/env ruby
# frozen_string_literal: true

# scripts/devenv.rb — this checkout's development environment.
#
#   rake dev:ports:ensure      allocate this checkout's port group if it has none (idempotent)
#   rake dev:ports:overwrite   deliberately move to a different group (stop services first)
#   rake dev:ports:show        print the allocation and the values derived from it
#
# TWO files, loaded together by mise (mise.toml [env]), because they have different owners:
#
#   .dev/ports.env     port-tamer's state file. NAME=<port> lines only — port-tamer rewrites it
#                      canonically from port-tamer.toml, and rejects the file outright if it holds
#                      anything else. Not ours to write.
#   .dev/derived.env   everything computed from those ports: the PG* client defaults and the
#                      default target's full PGX_TEST_* set (scripts/lib/test_targets.rb).
#
# None of the derived values can be static mise settings. The socket directory depends on the
# checkout's path length (DevPaths.socket_dir — sun_path is capped at 104 bytes on macOS) and on
# the platform (a Linux PGDATA is unreadable by a macOS server, so the devcontainer and a native
# checkout keep separate trees). Deriving them here, from the same module scripts/devdb.rb starts
# the servers with, is what keeps the server's socket and the client's PGHOST identical by
# construction rather than by two implementations agreeing.
#
# This also decodes the client TLS material into .dev/certs. The devcontainer did that in a
# postStartCommand and test.sh did it again, both writing to /tmp — a single machine-wide
# namespace that two checkouts running at once would fight over.

require "fileutils"
require "socket"
require_relative "lib/dev_paths"
require_relative "lib/test_targets"

PORT_TAMER_CONFIG = File.join(DevPaths::ROOT, "port-tamer.toml")

# Run port-tamer, distinguishing "not installed" from "ran and refused". Ruby's `system` returns
# nil when the command cannot be executed at all and false when it ran and exited nonzero — a
# distinction worth keeping, because the two have completely different fixes. port-tamer is pinned
# in mise.toml, so it is on PATH inside `mise run` / `mise exec` and in an activated shell.
def port_tamer(*args)
  ok = system("port-tamer", *args)
  abort <<~MSG if ok.nil?
    devenv: port-tamer not found. It is a project tool, pinned in mise.toml:
      mise install
  MSG
  ok
end

# Replacing the allocation also replaces PC_PORT_NUM, the only address the ordinary lifecycle
# commands retain for the supervisor. Doing that while it is listening would strand every process
# on the old port: the next `dev:down` would look only at the new one. Check the saved value
# directly rather than through DevPaths.port so `--overwrite` can still repair an incomplete or
# otherwise incompatible state file.
def refuse_live_overwrite!
  return unless File.exist?(DevPaths::PORTS_ENV)

  port = File.read(DevPaths::PORTS_ENV)[/^PC_PORT_NUM=(\d+)$/, 1]
  return unless port

  Socket.tcp("127.0.0.1", port.to_i, connect_timeout: 0.25) do
    abort <<~MSG
      devenv: refusing to replace the port allocation while this checkout's supervisor is
      listening on 127.0.0.1:#{port}. Stop it first:
          mise run dev:down
      Then re-run `mise run dev:ports:overwrite`.
    MSG
  end
rescue Errno::ECONNREFUSED
  nil
rescue SystemCallError, SocketError => e
  abort "devenv: could not verify whether 127.0.0.1:#{port} is in use (#{e.message}); " \
        "refusing to replace the allocation."
end

def allocate(overwrite:)
  FileUtils.mkdir_p(DevPaths::DEV_DIR) # port-tamer writes its state file, it does not create dirs
  refuse_live_overwrite! if overwrite

  argv = ["allocate", "--state-file", DevPaths::PORTS_ENV, PORT_TAMER_CONFIG]
  argv.insert(1, "--overwrite") if overwrite

  # The one case a user hits is a state file from an incompatible port-tamer.toml (entries
  # reordered or inserted), which is deliberately not replaced silently — name the way out.
  abort <<~MSG unless port_tamer(*argv)
    devenv: port-tamer could not allocate this checkout's ports (see the error above).
    If the saved allocation no longer matches port-tamer.toml, stop this checkout's services and
    replace it: `mise run dev:ports:overwrite`.
  MSG
end

# Decode the checked-in base64 certificates into this checkout's .dev/certs. Cheap and
# deterministic, so it runs on every ensure rather than needing its own task or process.
CLIENT_CERTS = {
  "ca.pem" => "ca.pem.b64",
  "pgx_sslcert.crt" => "pgx_sslcert.crt.b64",
  "pgx_sslcert.key" => "pgx_sslcert.key.b64",
}.freeze

def write_certs
  FileUtils.mkdir_p(DevPaths::CERTS_DIR)

  CLIENT_CERTS.each do |name, source|
    src = File.join(DevPaths::TESTSETUP_CERTS, source)
    abort "devenv: missing #{src}" unless File.exist?(src)

    dst = File.join(DevPaths::CERTS_DIR, name)
    decoded = File.read(src).unpack1("m")
    next if File.exist?(dst) && File.binread(dst) == decoded

    File.binwrite(dst, decoded)
  end

  # libpq refuses a client key that is group- or world-readable.
  File.chmod(0o600, File.join(DevPaths::CERTS_DIR, "pgx_sslcert.key"))
end

def derived_body
  vars = TestTargets.pg_defaults.merge(TestTargets.env_for(TestTargets.default))

  # Values are double-quoted. Most of these connection strings contain spaces, which mise's dotenv
  # parser rejects bare, and the CockroachDB URL contains `&` and `?`, which a shell would act on.
  # The escaping covers the characters that stay live inside double quotes when the file is
  # `source`d rather than parsed: backslash, quote, dollar and backtick.
  lines = vars.map { |name, value| %(#{name}="#{value.gsub(/([\\"$`])/, '\\\\\\1')}") }.join("\n")

  # Comments go on their OWN line: this file is read by mise's dotenv parser and by `source` in a
  # shell, and a trailing `VAR=value # note` is not portable across both.
  <<~ENV
    # Generated by scripts/devenv.rb from .dev/ports.env — DO NOT EDIT, and do not commit
    # (.dev/ is ignored). Regenerate with `mise run dev:ports:ensure`.
    #
    # The PG* client defaults point a bare psql at this checkout's default cluster over its Unix
    # socket. The PGX_TEST_* set is the #{TestTargets.default} target, so `go test ./...` with no
    # wrapper runs against the same server ./test.sh does by default. PGHOST and PGPORT are a
    # pair — never export one without the other.
    #
    # These paths are platform-specific (#{DevPaths::PLATFORM}). After switching a checkout
    # between native and the devcontainer, re-run `mise run dev:ports:ensure`.

    #{lines}
  ENV
end

def write_derived
  body = derived_body
  return if File.exist?(DevPaths::DERIVED_ENV) && File.read(DevPaths::DERIVED_ENV) == body

  FileUtils.mkdir_p(DevPaths::DEV_DIR)
  File.write(DevPaths::DERIVED_ENV, body)
end

def show
  port_tamer("status", "--state-file", DevPaths::PORTS_ENV, PORT_TAMER_CONFIG)
  puts
  puts "  platform      #{DevPaths::PLATFORM}"
  puts "  socket dir    #{DevPaths.socket_dir}"
  puts "  certs         #{DevPaths::CERTS_DIR}"
  puts
  puts "  TARGET   PORT    DATA"
  DevPaths::PG_MAJORS.each do |major|
    default = major == DevPaths::DEFAULT_PG_MAJOR ? " (default)" : ""
    puts format("  %-8s %-7s %s%s", "pg#{major}", DevPaths.pgport(major), DevPaths.pgdata(major), default)
  end
  puts format("  %-8s %-7s %s", "crdb", DevPaths.port("CRDB_PORT"), DevPaths.crdb_store)
  puts
  puts "  #{DevPaths::PORTS_ENV}"
  puts "  #{DevPaths::DERIVED_ENV}"
end

case ARGV[0]
when "ensure"
  allocate(overwrite: false)
  write_certs
  write_derived
when "overwrite"
  allocate(overwrite: true)
  write_certs
  write_derived
when "show"
  show
else
  abort("usage: devenv.rb ensure|overwrite|show")
end
