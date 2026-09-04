#!/usr/bin/env ruby
# frozen_string_literal: true

# Explicitly prewarm or stop database test targets. Tests use the same lifecycle code, but stop a
# target afterwards only when they had to start it themselves.

require_relative "lib/dev_services"

action = ARGV.shift
unless ["start", "stop"].include?(action)
  abort "usage: dev_services.rb start|stop [pg14|pg15|pg16|pg17|pg18|crdb|all ...]"
end

begin
  DevServices.targets(ARGV).each do |target|
    DevServices.with_lock(target) { DevServices.public_send(action, target) }
  end
rescue DevServices::Error => e
  warn "dev services: #{e.message}"
  exit 1
end
