package redis.clients.jedis;

import java.io.Closeable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class PrebuiltJedisShardInfo extends JedisShardInfo implements Closeable {

	private final Jedis jedis;

	public PrebuiltJedisShardInfo(Jedis jedis) {
		super(jedis.getClient().getHost(), jedis.getClient().getPort());
		this.jedis = jedis;
	}

	@Override
	public void close() {
		jedis.close();
	}

	@Override
	public Jedis createResource() {
		return jedis;
	}
}
