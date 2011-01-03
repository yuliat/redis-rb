# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))
require "redis/distributed"

setup do
  log = StringIO.new
  init Redis::Distributed.new(NODES, :logger => ::Logger.new(log))
end

load './test/lint/lists.rb'

test "RPOPLPUSH" do |r|
  assert_raise Redis::Distributed::CannotDistribute do
    r.rpoplpush("foo", "bar")
  end
end

test "BRPOP can block on multiple lists" do |r|
  r.rpush("{q}ueue1", "job1")
  r.rpush("{q}ueue2", "job2")

  t = Thread.new do
    sleep 0.3
    r.dup.rpush("{q}ueue3", "job3")
  end

  assert_equal ["{q}ueue1", "job1"], r.brpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)
  assert_equal ["{q}ueue2", "job2"], r.brpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)
  assert_equal ["{q}ueue3", "job3"], r.brpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)

  t.join

  assert_raise Redis::Distributed::CannotDistribute do
    r.brpop("queue1", "queue2", 1)
  end
end

test "BLPOP can block on multiple lists" do |r|
  r.lpush("{q}ueue1", "job1")
  r.lpush("{q}ueue2", "job2")

  t = Thread.new do
    sleep 0.3
    r.dup.lpush("{q}ueue3", "job3")
  end

  assert_equal ["{q}ueue1", "job1"], r.blpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)
  assert_equal ["{q}ueue2", "job2"], r.blpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)
  assert_equal ["{q}ueue3", "job3"], r.blpop("{q}ueue1", "{q}ueue2", "{q}ueue3", 1)

  t.join

  assert_raise Redis::Distributed::CannotDistribute do
    r.blpop("queue1", "queue2", 1)
  end
end
