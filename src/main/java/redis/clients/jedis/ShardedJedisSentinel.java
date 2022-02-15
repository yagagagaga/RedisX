package redis.clients.jedis;

import java.io.Serializable;
import java.util.List;

import redis.clients.jedis.util.Log;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

public class ShardedJedisSentinel extends ShardedJedis implements Serializable, Log {

	public ShardedJedisSentinel(List<JedisShardInfo> shards) {
		super(shards);
		info("ShardedJedisSentinel Connection ... ");
	}

	@Override
	public void close() {

		for (Jedis jedis : getAllShards()) {
			try {
				jedis.close();
			} catch (Exception ignored) {
				// ignore the exception node,
				// so that all other normal nodes can release all connections.
			}
		}
	}
}
