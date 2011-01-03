# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))
require File.expand_path("./redis_mock", File.dirname(__FILE__))

include RedisMock::Helper

require "redis/distributed"

setup do
  log = StringIO.new
  [init(Redis::Distributed.new(NODES, :logger => ::Logger.new(log))), log]
end

$TEST_PIPELINING = false
$TEST_INSPECT    = false

load File.expand_path("./lint/internals.rb", File.dirname(__FILE__))

test "can be dup'ed to create a new connection" do |r1, _|
  clients = r1.info[0]["connected_clients"].to_i

  r2 = r1.dup
  r2.ping

  assert_equal clients + 1, r1.info[0]["connected_clients"].to_i
end

test "keeps options after dup" do |r1, _|
  r1 = Redis::Distributed.new(NODES, :tag => /^(\w+):/)

  assert_raise(Redis::Distributed::CannotDistribute) do
    r1.sinter("foo", "bar")
  end

  assert_equal [], r1.sinter("baz:foo", "baz:bar")

  r2 = r1.dup

  assert_raise(Redis::Distributed::CannotDistribute) do
    r2.sinter("foo", "bar")
  end

  assert_equal [], r2.sinter("baz:foo", "baz:bar")
end
