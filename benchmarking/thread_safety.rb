# Run with
#
#   $ ruby -Ilib benchmarking/thread_safety.rb
#

begin
  require "bench"
rescue LoadError
  $stderr.puts "`gem install bench` and try again."
  exit 1
end

require "redis"

def stress(redis)
  redis.flushdb

  n = (ARGV.shift || 2000).to_i

  n.times do |i|
    key = "foo:#{i}"
    redis.set key, i
    redis.get key
  end
end

default = Redis.new
thread_safe = Redis.new(:thread_safe => true)

benchmark "Default" do
  stress(default)
end

benchmark "Thread-safe" do
  stress(thread_safe)
end

run 10
