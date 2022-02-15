package redis.clients.jedis.util;

public class RedisConstants {

	public static final String REDIS_MASTERS = "redis.masters";
	public static final String REDIS_SENTINEL = "redis.sentinel";
	public static final String REDIS_USER = "redis.user";
	public static final String REDIS_PASSWORD = "redis.password";

	public static final String MAX_WAIT_MILLIS = "redis.pool.maxWaitMillis";
	public static final String TEST_WHILE_IDLE = "redis.pool.testWhileIdle";
	public static final String TIME_BETWEEN_EVICTION_RUNS_MILLIS = "redis.pool.timeBetweenEvictionRunsMillis";
	public static final String NUM_TESTS_PER_EVICTION_RUN = "redis.pool.numTestsPerEvictionRun";
	public static final String MIN_EVICTABLE_IDLE_TIME_MILLIS = "redis.pool.minEvictableIdleTimeMillis";
	public static final String MAX_TOTAL = "redis.pool.maxTotal";
	public static final String MAX_IDLE = "redis.pool.maxIdle";
	public static final String MIN_IDLE = "redis.pool.minIdle";
	public static final String TEST_ON_BORROW = "redis.pool.testOnBorrow";
	public static final String TEST_ON_RETURN = "redis.pool.testOnReturn";
	public static final String CONNECTION_TIMEOUT = "redis.client.timeout";
}
