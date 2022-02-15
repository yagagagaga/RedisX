package redis.clients.jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import redis.clients.jedis.util.Log;
import redis.clients.jedis.util.PropertiesPlus;
import redis.clients.jedis.util.RedisConstants;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ShardedJedisSentinelPoolManager implements Log {

	private static transient JedisSentinelPools jedisSentinelPools = null;

	public static JedisPoolConfig shardedJedisConfig(PropertiesPlus props) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(true);
		int maxTotal = props.getInteger(RedisConstants.MAX_TOTAL, 10);
		config.setMaxTotal(maxTotal);
		int maxIdle = props.getInteger(RedisConstants.MAX_IDLE, 4);
		config.setMaxIdle(maxIdle);
		int minIdle = props.getInteger(RedisConstants.MIN_IDLE, 1);
		config.setMinIdle(minIdle);
		long maxWaitMillis = props.getLong(RedisConstants.MAX_WAIT_MILLIS, 10000L);
		config.setMaxWaitMillis(maxWaitMillis);
		boolean testOnBorrow = props.getBoolean(RedisConstants.TEST_ON_BORROW, true);
		config.setTestOnBorrow(testOnBorrow);
		boolean testOnReturn = props.getBoolean(RedisConstants.TEST_ON_RETURN, true);
		config.setTestOnReturn(testOnReturn);
		boolean testWhileIdle = props.getBoolean(RedisConstants.TEST_WHILE_IDLE, false);
		config.setTestWhileIdle(testWhileIdle);
		long minEvictableIdleTimeMillis = props.getLong(RedisConstants.MIN_EVICTABLE_IDLE_TIME_MILLIS, 60000L);
		config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
		long timeBetweenEvictionRunsMillis = props.getLong(RedisConstants.TIME_BETWEEN_EVICTION_RUNS_MILLIS, 30000L);
		config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
		int numTestsPerEvictionRun = props.getInteger(RedisConstants.NUM_TESTS_PER_EVICTION_RUN, -1);
		config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
		return config;
	}

	private static JedisSentinelPools makeShardedSentinelJedisPools() {
		return makeShardedSentinelJedisPools("redis_cluster.properties");
	}

	private static JedisSentinelPools makeShardedSentinelJedisPools(String path) {
		log.info("makeShardedSentinelJedisPools config file is: {}", path);

		PropertiesPlus props = PropertiesPlus.load(path);
		GenericObjectPoolConfig<?> config = shardedJedisConfig(props);
		List<String> redisMasters = props.getList(RedisConstants.REDIS_MASTERS, ",");
		Set<String> redisSentinel = new HashSet<>(Arrays.asList(props.getProperty(RedisConstants.REDIS_SENTINEL).split(",")));
		final String timeout = props.getProperty(RedisConstants.CONNECTION_TIMEOUT, "30000");
		final String password = props.getProperty(RedisConstants.REDIS_PASSWORD, null);
		final String user = props.getProperty(RedisConstants.REDIS_USER, null);
		return new JedisSentinelPools(redisMasters, redisSentinel, config, Integer.parseInt(timeout), user, password, Protocol.DEFAULT_DATABASE);
	}

	public static JedisSentinelPools getshardedJedisPools() {
		return getshardedJedisPools("redis_cluster.properties");
	}

	public static JedisSentinelPools getshardedJedisPools(String path) {
		if (jedisSentinelPools == null) {
			synchronized (ShardedJedisSentinelPoolManager.class) {
				if (jedisSentinelPools == null) {
					jedisSentinelPools = makeShardedSentinelJedisPools(path);
				}
			}
		}
		return jedisSentinelPools;
	}

}
