require File.expand_path("./../redis_mock", File.dirname(__FILE__))

include RedisMock::Helper

test "BLPOP" do |r|
  r.lpush("foo", "s1")
  r.lpush("foo", "s2")

  thread = Thread.new do
    sleep 0.3
    r.dup.lpush("foo", "s3")
  end

  assert ["foo", "s2"] == r.blpop("foo", 1)
  assert ["foo", "s1"] == r.blpop("foo", 1)
  assert ["foo", "s3"] == r.blpop("foo", 1)

  thread.join
end

test "BRPOP" do |r|
  r.rpush("foo", "s1")
  r.rpush("foo", "s2")

  t = Thread.new do
    sleep 0.3
    r.dup.rpush("foo", "s3")
  end

  assert ["foo", "s2"] == r.brpop("foo", 1)
  assert ["foo", "s1"] == r.brpop("foo", 1)
  assert ["foo", "s3"] == r.brpop("foo", 1)

  t.join
end

test "RPUSH" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert 2 == r.llen("foo")
  assert "s2" == r.rpop("foo")
end

test "LPUSH" do |r|
  r.lpush "foo", "s1"
  r.lpush "foo", "s2"

  assert 2 == r.llen("foo")
  assert "s2" == r.lpop("foo")
end

test "LLEN" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert 2 == r.llen("foo")
end

test "LRANGE" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"
  r.rpush "foo", "s3"

  assert ["s2", "s3"] == r.lrange("foo", 1, -1)
  assert ["s1", "s2"] == r.lrange("foo", 0, 1)

  assert [] == r.lrange("bar", 0, -1)
end

test "LTRIM" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"
  r.rpush "foo", "s3"

  r.ltrim "foo", 0, 1

  assert 2 == r.llen("foo")
  assert ["s1", "s2"] == r.lrange("foo", 0, -1)
end

test "LINDEX" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert "s1" == r.lindex("foo", 0)
  assert "s2" == r.lindex("foo", 1)
end

test "LSET" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert "s2" == r.lindex("foo", 1)
  assert r.lset("foo", 1, "s3")
  assert "s3" == r.lindex("foo", 1)

  assert_raise RuntimeError do
    r.lset("foo", 4, "s3")
  end
end

test "LREM" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert 1 == r.lrem("foo", 1, "s1")
  assert ["s2"] == r.lrange("foo", 0, -1)
end

test "LPOP" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert 2 == r.llen("foo")
  assert "s1" == r.lpop("foo")
  assert 1 == r.llen("foo")
end

test "RPOP" do |r|
  r.rpush "foo", "s1"
  r.rpush "foo", "s2"

  assert 2 == r.llen("foo")
  assert "s2" == r.rpop("foo")
  assert 1 == r.llen("foo")
end

test "BLPOP should try to reconnect when disconnected by Redis" do
  times = 0

  replies = {
    :blpop => lambda do |*_|
      times += 1
      "+OK" if times > 2
    end
  }

  redis_mock(replies) do
    assert_equal "OK", Redis.new(OPTIONS.merge(:port => 6380)).blpop("foo", 0)
  end
end

test "BLPOP with a timeout should return nil when hitting the timeout" do |r|
  assert_equal nil, r.blpop("foo", 1)
end

test "BLPOP losing connection" do
  replies = {
    :blpop => lambda { |*_| Process.kill(9, Process.pid) },
    :ping  => lambda { |*_| "+PONG" },
  }

  redis_mock(replies) do
    redis = Redis.new(OPTIONS.merge(:port => 6380))

    assert_equal "PONG", redis.ping

    assert_raise(Errno::ECONNREFUSED) do
      redis.blpop "foo", 1
    end
  end
end
