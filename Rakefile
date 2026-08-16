require "erb"
require "fileutils"

rule '.go' => '.go.erb' do |task|
  erb = ERB.new(File.read(task.source))
  File.write(task.name, "// Code generated from #{task.source}. DO NOT EDIT.\n\n" + erb.result(binding))
  sh "goimports", "-w", task.name
end

generated_code_files = [
  "pgtype/int.go",
  "pgtype/int_test.go",
  "pgtype/integration_benchmark_test.go",
  "pgtype/zeronull/int.go",
  "pgtype/zeronull/int_test.go"
]

desc "Generate code"
task generate: generated_code_files

# references:* — provision local, read-only checkouts of reference material used
# when building pgx (currently the PostgreSQL source tree).
#
# Storage model:
#   * A bare `--mirror` clone of each repo lives on the devcontainer's shared
#     persist volume under MIRROR_ROOT. It holds the full history + all
#     branches/tags, is downloaded once, and survives container rebuilds. It is
#     the canonical copy, shared across every container for this project.
#   * Each container creates a lightweight shared clone in ./references/<name>.
#     Its Git metadata is local to that checkout while its object store borrows
#     from the mirror. Containers can therefore use the same in-container path
#     without sharing Git worktree registrations or downloading history again.
#
# Provisioning a new container is therefore cheap: a local clone from the
# already-present mirror, with no network fetch or duplicate object store.

# Each entry is one reference repo. `ref` is the branch/tag checked out into the
# local checkout. PostgreSQL is pinned to REL_18_STABLE to match the devcontainer's
# default PG18 test target.
REFERENCE_REPOS = [
  { name: "postgres", url: "https://github.com/postgres/postgres.git", ref: "REL_18_STABLE", license: "PostgreSQL License" },
].freeze

# Canonical mirrors live on the shared persist volume by default; overridable
# for use outside the devcontainer.
MIRROR_ROOT   = ENV.fetch("REFERENCES_MIRROR_DIR", "/persist/shared/references")
CHECKOUT_ROOT = File.join(__dir__, "references")

def mirror_path(repo)   = File.expand_path(File.join(MIRROR_ROOT, "#{repo[:name]}.git"))
def checkout_path(repo) = File.join(CHECKOUT_ROOT, repo[:name])

# File.exist? is false for dangling symlinks, which are still leftovers setup
# must replace (and clean must remove).
def path_present?(path) = File.exist?(path) || File.symlink?(path)

# Run a git command against a bare mirror. Names the gitdir explicitly and lifts
# any safe.bareRepository=explicit guard. Raises on failure.
def git_bare(repo, *args)
  sh "git", "-c", "safe.bareRepository=all", "--git-dir", mirror_path(repo), *args
end

# Capture stdout of a command given as an argv array (no shell parsing, no
# quoting pitfalls). Returns [stdout_string, success_boolean].
def capture(*args)
  out = IO.popen(args, err: File::NULL, &:read)
  [out.to_s, $?.success?]
end

# A managed checkout has its own repository metadata, uses this exact mirror as
# its origin, and borrows that mirror's object database. This rejects ordinary
# clones, linked worktrees left by the old implementation, and dangling gitfiles.
def checkout_valid?(repo)
  wp = checkout_path(repo)
  git_dir = File.join(wp, ".git")
  objects_dir = File.join(git_dir, "objects")
  alternates_path = File.join(objects_dir, "info", "alternates")

  return false unless File.directory?(git_dir) && File.file?(alternates_path)

  inside, inside_ok = capture("git", "-C", wp, "rev-parse", "--is-inside-work-tree")
  origin, origin_ok = capture("git", "-C", wp, "remote", "get-url", "origin")
  return false unless inside_ok && inside.strip == "true" && origin_ok
  return false unless File.identical?(File.expand_path(origin.strip, wp), mirror_path(repo))

  File.foreach(alternates_path).any? do |alternate|
    alternate = alternate.strip
    next false if alternate.empty?

    File.identical?(File.expand_path(alternate, objects_dir), File.join(mirror_path(repo), "objects"))
  end
rescue SystemCallError
  false
end

# A mirror is valid only if it exists AND has at least one ref — this rejects a
# directory left behind by an interrupted clone (which exists but is incomplete).
def mirror_valid?(repo)
  return false unless File.directory?(mirror_path(repo))
  out, ok = capture("git", "-c", "safe.bareRepository=all", "--git-dir", mirror_path(repo), "for-each-ref", "--count=1")
  ok && !out.strip.empty?
end

# Clone the bare mirror onto the persist volume if it is missing or broken.
def ensure_mirror(repo)
  if mirror_valid?(repo)
    puts "  mirror cached: #{mirror_path(repo)}"
  else
    FileUtils.rm_rf(mirror_path(repo)) # clear any partial/broken clone
    puts "  cloning mirror (full history): #{repo[:url]}"
    sh "git", "clone", "--mirror", repo[:url], mirror_path(repo)
  end
end

# Serialize mirror creation and updates across devcontainer instances. Local
# checkouts do not share metadata, but they all read from the same object store.
def with_mirror_lock(repo)
  FileUtils.mkdir_p(MIRROR_ROOT)
  File.open(File.join(MIRROR_ROOT, ".#{repo[:name]}.lock"), File::RDWR | File::CREAT, 0o644) do |lock|
    lock.flock(File::LOCK_EX)
    yield
  end
end

# Check out (or re-point) the local clone at the configured ref. Resolve the
# commit in the freshly updated mirror so an existing checkout cannot remain
# stale merely because only the mirror was fetched.
def ensure_checkout(repo)
  wp  = checkout_path(repo)
  ref = repo[:ref]

  commit, ok = capture("git", "-c", "safe.bareRepository=all", "--git-dir", mirror_path(repo),
                       "rev-parse", "--verify", "#{ref}^{commit}")
  raise "ref #{ref.inspect} not found in #{mirror_path(repo)}" unless ok && !commit.strip.empty?

  if checkout_valid?(repo)
    puts "  checkout present: #{wp} -> #{ref}"
  else
    puts "  replacing invalid checkout: #{wp}" if path_present?(wp)
    FileUtils.rm_rf(wp) if path_present?(wp)
    puts "  cloning checkout: #{wp} -> #{ref}"
    sh "git", "clone", "--shared", "--no-checkout", mirror_path(repo), wp
    raise "checkout at #{wp} is not linked to #{mirror_path(repo)}" unless checkout_valid?(repo)
  end

  sh "git", "-C", wp, "checkout", "--detach", commit.strip
end

# Run a block per repo, collecting failures so one bad repo does not abort the rest.
def for_each_repo
  failures = []
  REFERENCE_REPOS.each do |repo|
    puts "#{repo[:name]}:"
    begin
      yield repo
    rescue => e
      warn "  FAILED: #{e.message}"
      failures << repo[:name]
    end
  end
  abort "references: failed for #{failures.join(', ')}" unless failures.empty?
end

namespace :references do
  desc "Clone/refresh reference mirrors on persist and create checkouts in references/"
  task :setup do
    FileUtils.mkdir_p(MIRROR_ROOT)
    FileUtils.mkdir_p(CHECKOUT_ROOT)
    for_each_repo do |repo|
      with_mirror_lock(repo) do
        ensure_mirror(repo)
        ensure_checkout(repo)
      end
    end
    puts
    puts "Done. Reference sources are in #{CHECKOUT_ROOT}"
    Rake::Task["references:status"].invoke
  end

  desc "Fetch latest upstream for all mirrors and re-point checkouts"
  task :update do
    for_each_repo do |repo|
      with_mirror_lock(repo) do
        abort "mirror missing for #{repo[:name]}; run `rake references:setup`" unless mirror_valid?(repo)
        git_bare(repo, "remote", "update", "--prune")
        ensure_checkout(repo)
      end
    end
  end

  desc "Show provisioned reference repos, their pinned ref, and current HEAD"
  task :status do
    puts
    puts format("  %-16s %-14s %-14s %s", "REPO", "REF", "HEAD", "LICENSE")
    REFERENCE_REPOS.each do |repo|
      wp = checkout_path(repo)
      state =
        if checkout_valid?(repo)
          head, ok = capture("git", "-C", wp, "rev-parse", "--short", "HEAD")
          ok && !head.strip.empty? ? head.strip : "(invalid)"
        elsif path_present?(wp)
          "(invalid)"
        else
          "(not set up)"
        end
      puts format("  %-16s %-14s %-14s %s", repo[:name], repo[:ref], state, repo[:license])
    end
    puts
    puts "  mirrors:   #{MIRROR_ROOT}"
    puts "  checkouts: #{CHECKOUT_ROOT}"
  end

  desc "Remove checkouts from references/ (keeps the cached mirrors on persist)"
  task :clean do
    REFERENCE_REPOS.each do |repo|
      wp = checkout_path(repo)
      next unless path_present?(wp)
      puts "removing checkout: #{wp}"
      FileUtils.rm_rf(wp)
    end
  end
end
