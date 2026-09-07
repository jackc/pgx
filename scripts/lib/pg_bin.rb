# frozen_string_literal: true

# scripts/lib/pg_bin.rb — locate the PostgreSQL SERVER binaries (initdb, postgres, pg_ctl) for a
# given major version.
#
# The client tools (psql, pg_isready) are on PATH on every platform we care about. The server
# tools usually are NOT, and for different reasons on each:
#
#   * Debian/Ubuntu (pgdg)  — postgresql-common puts version-multiplexing wrappers in /usr/bin for
#                             the CLIENT tools only; initdb/postgres/pg_ctl live in
#                             /usr/lib/postgresql/<major>/bin and are reached via pg_ctlcluster.
#   * macOS (Homebrew)      — postgresql@<major> is KEG-ONLY, so nothing is linked into the prefix
#                             bin; the tools sit in <brew prefix>/opt/postgresql@<major>/bin.
#
# So a portable `initdb` needs an explicit search rather than a bare exec. Order: an explicit
# PGBIN_<major> override, then the two known layouts for that major, then PATH. Failing all of
# that we say what we looked for and how to install it, because "initdb: not found" is a uniquely
# unhelpful error.
#
# EVERY candidate is version-checked. pgx runs five majors side by side, so a directory holding
# the right tool names is not enough: a bare PATH `initdb` from postgresql@18 would happily create
# a PG14 cluster that the PG14 server then refuses to start. PATH is searched LAST for the same
# reason — the explicitly versioned locations are the trustworthy ones.

require "rbconfig"
require "shellwords"

module PgBin
  module_function

  TOOLS = %w[initdb postgres pg_ctl].freeze

  DARWIN = !(RbConfig::CONFIG["host_os"] =~ /darwin/).nil?

  # The directory holding the server binaries for `major`, or nil.
  def dir(major)
    explicit = ENV["PGBIN_#{major}"]
    return explicit if explicit && !explicit.empty? && usable?(explicit, major)

    from_versioned = candidates(major).find { |d| usable?(d, major) }
    return from_versioned if from_versioned

    from_path = which("initdb")
    parent = from_path && File.dirname(from_path)
    parent if parent && usable?(parent, major)
  end

  # The path to one tool, aborting with an actionable message if the toolchain is missing.
  def tool(name, major)
    d = dir(major) or abort(missing_message(major))
    File.join(d, name)
  end

  def usable?(directory, major) = complete?(directory) && major?(directory, major)

  def complete?(d) = TOOLS.all? { |t| File.executable?(File.join(d, t)) }

  # `initdb (PostgreSQL) 18.2` / `initdb (PostgreSQL) 14.20`. Anything unparseable is treated as a
  # mismatch: an unidentifiable toolchain is not one to build a cluster with.
  def major?(directory, major)
    out = `#{File.join(directory, 'initdb').shellescape} --version 2>/dev/null`
    $?.success? && out[/\s(\d+)[.\s]/, 1].to_i == major.to_i
  rescue SystemCallError
    false
  end

  def candidates(major)
    if DARWIN
      # `brew --prefix` is authoritative but slow to shell out to on every call; the two standard
      # prefixes (Apple silicon, Intel) cover any default install, and PGBIN_<major> covers the
      # rest.
      %W[/opt/homebrew/opt/postgresql@#{major}/bin /usr/local/opt/postgresql@#{major}/bin]
    else
      %W[/usr/lib/postgresql/#{major}/bin /usr/pgsql-#{major}/bin]
    end
  end

  def which(name)
    ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).each do |d|
      candidate = File.join(d, name)
      return candidate if File.executable?(candidate) && !File.directory?(candidate)
    end
    nil
  end

  def install_hint(major)
    DARWIN ? "brew install postgresql@#{major}" : "apt-get install postgresql-#{major}"
  end

  def missing_message(major)
    <<~MSG
      PostgreSQL #{major} server binaries not found (need: #{TOOLS.join(', ')}).
      Looked in: $PGBIN_#{major}, #{candidates(major).join(', ')}, $PATH
      Install with: #{install_hint(major)}
      Or point PGBIN_#{major} at the directory holding them.

      See DEVELOPMENT.md for the full prerequisite list.
    MSG
  end
end
