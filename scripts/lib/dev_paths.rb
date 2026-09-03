# frozen_string_literal: true

# scripts/lib/dev_paths.rb — the filesystem layout of a development checkout's instance-local
# state, in ONE place because several tools must agree on it exactly:
#
#   scripts/devenv.rb   writes PGHOST and the PGX_TEST_* strings out of these paths
#   scripts/devdb.rb    starts each server on that socket directory and probes it
#   scripts/dev.rb      reports it
#
# If these ever disagreed the failure would be a connection to a socket that does not exist,
# which is precisely the bug this module exists to make impossible.

require "digest"
require "fileutils"
require "rbconfig"

module DevPaths
  module_function

  ROOT = File.expand_path("../..", __dir__)
  DEV_DIR = File.join(ROOT, ".dev")

  # Two dotenv files, both loaded by mise (mise.toml [env]) — see scripts/devenv.rb for why they
  # are separate: port-tamer owns the first and accepts nothing but NAME=<port> in it.
  PORTS_ENV = File.join(DEV_DIR, "ports.env")     # port-tamer's state: the port assignments
  DERIVED_ENV = File.join(DEV_DIR, "derived.env") # ours: everything computed from them

  # The client TLS material the pgx TLS tests point at. The devcontainer and test.sh decoded these
  # to /tmp, which is a single machine-wide namespace: two checkouts running at once overwrite
  # each other's copies. Per-checkout is the whole point of .dev/.
  CERTS_DIR = File.join(DEV_DIR, "certs")
  LOGS_DIR = File.join(DEV_DIR, "logs")

  # The checked-in, base64-encoded CA/server/client certificates these are decoded from.
  TESTSETUP_CERTS = File.join(ROOT, "testsetup", "certs")

  # The PostgreSQL majors this project tests against, oldest first. This list is the source for
  # port-tamer.toml's PGPORT_<major> entries, process-compose.yaml's processes, and the pg<major>
  # test targets — keep the three in step.
  PG_MAJORS = [14, 15, 16, 17, 18].freeze

  # The major a bare `go test ./...` and a bare `./test.sh` run against.
  DEFAULT_PG_MAJOR = 18

  # A PGDATA is not portable across platforms: a cluster initdb'd by the Linux devcontainer cannot
  # be read by a native macOS server, and one checkout may be opened both ways. Keying the runtime
  # state on os-arch lets both coexist instead of corrupting one another.
  PLATFORM = "#{RbConfig::CONFIG['host_os'].sub(/\d+(\.\d+)*$/, '')}-#{RbConfig::CONFIG['host_cpu']}"

  # Where a server's own runtime state lives: the clusters, the CockroachDB store, and the Unix
  # socket directory. Normally .dev/, next to everything else — but PGX_DEV_RUNTIME_DIR moves it,
  # and the devcontainer sets it (.devcontainer/devcontainer.json) to a path on its own named
  # volume.
  #
  # WHY the container needs that: .dev/ is inside the /workspaces/pgx host bind mount, and a bind
  # mount is the one filesystem these two things cannot rely on. Docker Desktop's macOS/Windows
  # sharing layer rejects the ownership and permission changes initdb makes ("could not change
  # permissions of directory", "data directory has invalid permissions"), and Unix socket creation
  # in it is unreliable. The per-version PostgreSQL containers this replaces used named volumes
  # for exactly the same two directories.
  #
  # Everything else in .dev/ — ports.env, derived.env, certs, logs — is ordinary file I/O and
  # stays in the checkout, where it belongs and where a human can find it.
  def runtime_dir
    override = ENV["PGX_DEV_RUNTIME_DIR"]
    override.nil? || override.empty? ? DEV_DIR : File.expand_path(override)
  end

  def platform_dir = File.join(runtime_dir, PLATFORM)

  # One cluster per major, all under this platform's directory.
  def pgdata(major) = File.join(platform_dir, "postgres", major.to_s, "data")

  def crdb_store = File.join(platform_dir, "crdb")

  # A Unix socket path is capped by sockaddr_un.sun_path — 104 bytes on macOS, 108 on Linux — and
  # PostgreSQL appends "/.s.PGSQL.<port>" to the directory. A checkout nested under a long home
  # directory blows that limit, and the resulting error points at nothing useful.
  SUN_PATH_MAX = RbConfig::CONFIG["host_os"] =~ /darwin/ ? 104 : 108

  # The directory every cluster puts its Unix socket in — ONE directory shared by all five majors,
  # the way the devcontainer's pg-sockets volume was, because PGX_TEST_UNIX_SOCKET_CONN_STRING
  # names a directory and finds the server by port.
  #
  # Inside .dev when it fits, otherwise a short deterministic /tmp path so the same checkout
  # always resolves to the same directory. The longest port is 5 digits.
  def socket_dir
    preferred = File.join(platform_dir, "postgres", "run")
    return preferred if preferred.bytesize + "/.s.PGSQL.00000".bytesize < SUN_PATH_MAX

    File.join("/tmp", "pgx-pg-#{Digest::SHA256.hexdigest("#{ROOT}\0#{PLATFORM}")[0, 12]}")
  end

  # Create the socket directory, private to this user, and refuse to use one that is not.
  #
  # The fallback above lands in /tmp, which on a shared host is a single machine-wide namespace:
  # its name is a hash of two guessable inputs, so another local user could pre-create it and then
  # sit between every local client and this checkout's servers — PGHOST and
  # PGX_TEST_UNIX_SOCKET_CONN_STRING both name this directory. mkdir_p's mode applies only to
  # directories it creates, hence the explicit check on one that already exists.
  def ensure_socket_dir!
    dir = socket_dir
    FileUtils.mkdir_p(dir, mode: 0o700)

    stat = File.stat(dir)
    if stat.uid != Process.uid
      abort "pgx dev: socket directory #{dir} belongs to uid #{stat.uid}, not to you " \
            "(uid #{Process.uid}). Remove it and try again."
    end
    File.chmod(0o700, dir) unless (stat.mode & 0o077).zero?

    dir
  end

  # Read one port out of port-tamer's state file. No default: guessing would silently collide with
  # another checkout, which is the exact failure the allocation prevents.
  def port(name)
    unless File.exist?(PORTS_ENV)
      abort "pgx dev: no port allocation — run `mise run dev:ports:ensure` (#{PORTS_ENV} is missing)."
    end

    File.read(PORTS_ENV)[/^#{Regexp.escape(name)}=(\d+)$/, 1] ||
      abort("pgx dev: #{name} missing from #{PORTS_ENV} — re-run `mise run dev:ports:ensure`.")
  end

  def pgport(major) = port("PGPORT_#{major}")
end
