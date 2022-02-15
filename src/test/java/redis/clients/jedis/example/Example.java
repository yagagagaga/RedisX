package redis.clients.jedis.example;

import java.util.List;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisSentinel;
import redis.clients.jedis.ShardedJedisSentinelPoolManager;

public class Example {
	public static void main(String[] args) {
		List<JedisShardInfo> shards = ShardedJedisSentinelPoolManager.getshardedJedisPools().getShards();
		ShardedJedisSentinel shardJedis = new ShardedJedisSentinel(shards);
		shardJedis.set("foo", "bar");
		String value = shardJedis.get("foo");
		System.out.println(value);
	}
}
