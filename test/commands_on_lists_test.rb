# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))

setup do
  init Redis.new(OPTIONS)
end

load './test/lint/lists.rb'

test "RPUSHX" do |r|
  r.rpushx "foo", "s1"
  r.rpush "foo", "s2"
  r.rpushx "foo", "s3"

  assert 2 == r.llen("foo")
  assert ["s2", "s3"] == r.lrange("foo", 0, -1)
end

test "LPUSHX" do |r|
  r.lpushx "foo", "s1"
  r.lpush "foo", "s2"
  r.lpushx "foo", "s3"

  assert 2 == r.llen("foo")
  assert ["s3", "s2"] == r.lrange("foo", 0, -1)
end

test "LINSERT" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s3"
  r.linsert "foo", :before, "s3", "s2"

  assert ["s1", "s2", "s3"] == r.lrange("foo", 0, -1)

  assert_raise(RuntimeError) do
    r.linsert "foo", :anywhere, "s3", "s2"
  end
end

test "RPOPLPUSH" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert "s2" == r.rpoplpush("foo", "bar")
  assert ["s2"] == r.lrange("bar", 0, -1)
  assert "s1" == r.rpoplpush("foo", "bar")
  assert ["s1", "s2"] == r.lrange("bar", 0, -1)
end

test "BLPOP can block on multiple lists" do |r|
  r.lpush("queue1", "job1")
  r.lpush("queue2", "job2")

  t = Thread.new do
    sleep 0.3
    r.dup.lpush("queue3", "job3")
  end

  assert_equal ["queue1", "job1"], r.blpop("queue1", "queue2", "queue3", 1)
  assert_equal ["queue2", "job2"], r.blpop("queue1", "queue2", "queue3", 1)
  assert_equal ["queue3", "job3"], r.blpop("queue1", "queue2", "queue3", 1)

  t.join
end

test "BRPOP should restore the timeout even if the command fails" do |r|
  r.incr "foo"

  assert_raise RuntimeError do
    r.brpop("foo", 1)
  end

  # TODO: should be testing a read timeout.
  assert_equal OPTIONS[:timeout], r.client.timeout
end
