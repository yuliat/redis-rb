# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))
require "redis/distributed"

setup do
  log = StringIO.new
  init Redis::Distributed.new(NODES, :logger => ::Logger.new(log))
end

test "BRPOP should unset a configured socket timeout" do |r|
  r = Redis::Distributed.new(NODES, :timeout => 1)

  assert_nothing_raised do
    r.brpop("foo", 2)
  end # Errno::EAGAIN raised if socket times out before redis command times out

  assert r.nodes.all? { |node| node.client.timeout == 1 }
end

