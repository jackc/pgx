# frozen_string_literal: true

# Start and stop the database servers supervised by this checkout's process-compose instance.
# PostgreSQL 18 is the always-on development default; every other test target is disabled at
# supervisor startup and can be started manually. Test runs use with_temporary_target so a server
# they start is stopped afterwards, while a server a developer prewarmed is left alone.

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require_relative "dev_paths"
require_relative "pg_bin"
require_relative "test_targets"

module DevServices
  module_function

  START_TIMEOUT = 90

  class Error < StandardError; end

  def targets(args)
    return [TestTargets.default] if args.empty?
    return TestTargets.names if args == ["all"]

    if args.include?("all")
      raise Error, "'all' cannot be combined with individual database targets"
    end

    invalid = args.reject { |target| TestTargets.valid?(target) }
    unless invalid.empty?
      raise Error, "unknown target#{invalid.one? ? '' : 's'}: #{invalid.join(', ')} " \
                   "(valid: #{(TestTargets.names + ['all']).join(', ')})"
    end

    args.uniq
  end

  def with_lock(target)
    lock_dir = File.join(DevPaths::DEV_DIR, "locks")
    FileUtils.mkdir_p(lock_dir)

    File.open(File.join(lock_dir, "#{target}.lock"), File::RDWR | File::CREAT, 0o600) do |lock|
      lock.flock(File::LOCK_EX)
      yield
    end
  end

  def states
    out, status = Open3.capture2e("process-compose", "process", "list", "--output", "json")
    unless status.success?
      raise Error, <<~MSG.chomp
        the development supervisor is not running.
        Start PostgreSQL 18 and the on-demand database supervisor with:
            mise run dev
        Or, detached:
            mise run dev -- -D && mise run dev:wait
      MSG
    end

    JSON.parse(out)
  rescue Errno::ENOENT
    raise Error, "process-compose is not installed; run `mise install`"
  rescue JSON::ParserError => e
    raise Error, "could not read process-compose state: #{e.message}"
  end

  def running?(target)
    state = states.find { |process| process.fetch("name") == target }
    raise Error, "process-compose has no process named #{target.inspect}" unless state

    state.fetch("is_running")
  end

  def probe(target, command)
    script, args =
      if target == "crdb"
        ["devcrdb.rb", []]
      else
        ["devdb.rb", [TestTargets.major_for(target).to_s]]
      end

    system(RbConfig.ruby, File.join(DevPaths::ROOT, "scripts", script), command, *args,
           out: File::NULL, err: File::NULL)
  end

  def wait_for(target, command)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + START_TIMEOUT

    until probe(target, command)
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        raise Error, "#{TestTargets.label(target)} did not become ready within #{START_TIMEOUT}s; " \
                     "inspect `process-compose process logs #{target}`"
      end

      sleep 0.1
    end
  end

  def require_available!(target)
    return if target == "crdb"

    major = TestTargets.major_for(target)
    return if PgBin.dir(major)

    raise Error, "PostgreSQL #{major} is not installed. #{PgBin.install_hint(major)}"
  end

  def prepare(target)
    # devcrdb.rb's ready command creates pgx_test when the in-memory node has just started.
    wait_for(target, "ready")

    if target != "crdb" && !probe(target, "testready")
      major = TestTargets.major_for(target)
      ok = system(RbConfig.ruby, File.join(DevPaths::ROOT, "scripts", "devdb.rb"),
                  "setup", major.to_s)
      raise Error, "could not set up #{TestTargets.label(target)}" unless ok
    end

    wait_for(target, "testready")
  end

  # Ensure a target is ready and leave it running. Returns true when this call started the server.
  def start(target)
    require_available!(target)
    started_here = !running?(target)

    if started_here
      puts "  #{target}: starting #{TestTargets.label(target)}"
      ok = system("process-compose", "process", "start", target)
      raise Error, "could not start #{TestTargets.label(target)}" unless ok
    end

    prepare(target)
    puts "  #{target}: ready"
    started_here
  end

  def stop(target)
    return false unless running?(target)

    puts "  #{target}: stopping #{TestTargets.label(target)}"
    ok = system("process-compose", "process", "stop", target)
    raise Error, "could not stop #{TestTargets.label(target)}" unless ok

    true
  end

  # Hold the target lock for the entire test run. Tests against one database cannot safely overlap,
  # and this also prevents one run from stopping a lazily started server while another is using it.
  def with_temporary_target(target)
    with_lock(target) do
      started_here = !running?(target)

      begin
        start(target)
        yield
      ensure
        stop(target) if started_here
      end
    end
  end
end
