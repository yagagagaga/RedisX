package redis.clients.jedis.client;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.JedisSentinelPools;
import redis.clients.jedis.util.PropertiesPlus;

public class JedisClientConfig {

	public final PropertiesPlus prop;
	
	private final Set<String> sentinels;
	private final String sentinelPassword;
	private final Duration sentinelConnectionTimeout;
	private final Duration sentinelSoTimeout;
	private final String sentinelClientName;

	private final int poolMinIdle;
	private final int poolMaxIdle;
	private final int poolMaxTotal;
	private final boolean poolBlockWhenExhausted;

	private final List<String> masterNames;
	private final String password;
	private final int database;
	private final Duration connectionTimeout;
	private final Duration soTimeout;
	private final String clientName;

	public JedisClientConfig(String config) {
		prop = PropertiesPlus.load(config);
		sentinels = prop.getSet("redis.sentinels", ",");
		sentinelPassword = prop.getString("redis.sentinel-password", null);
		sentinelConnectionTimeout = prop.getDuration("redis.sentinel-connection-timeout", Duration.ofMillis(Protocol.DEFAULT_TIMEOUT));
		sentinelSoTimeout = prop.getDuration("redis.sentinel-so-timeout", Duration.ofMillis(Protocol.DEFAULT_TIMEOUT));
		sentinelClientName = prop.getString("redis.sentinel-client-name", null);
		poolMinIdle = prop.getInteger("redis.pool-min-idle", GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
		poolMaxIdle = prop.getInteger("redis.pool-max-idle", GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
		poolMaxTotal = prop.getInteger("redis.pool-max-total", GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
		poolBlockWhenExhausted = prop.getBoolean("redis.pool-block-when-exhausted", GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);
		masterNames = prop.getList("redis.master-names", ",");
		password = prop.getString("redis.password", null);
		database = prop.getInteger("redis.database", Protocol.DEFAULT_DATABASE);
		connectionTimeout = prop.getDuration("redis.connection-timeout", Duration.ofMillis(Protocol.DEFAULT_TIMEOUT));
		soTimeout = prop.getDuration("redis.so-timeout", Duration.ofMillis(Protocol.DEFAULT_TIMEOUT));
		clientName = prop.getString("redis.client-name", null);
	}

	public JedisSentinelPools jedisSentinelPools() {
		GenericObjectPoolConfig<?> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMinIdle(poolMinIdle);
		poolConfig.setMaxIdle(poolMaxIdle);
		poolConfig.setMaxTotal(poolMaxTotal);
		poolConfig.setBlockWhenExhausted(poolBlockWhenExhausted);
		return new JedisSentinelPools(masterNames, sentinels, poolConfig, (int) connectionTimeout.toMillis(), (int) soTimeout.toMillis(),
				password, database, clientName, (int) sentinelConnectionTimeout.toMillis(), (int) sentinelSoTimeout.toMillis(), sentinelPassword,
				sentinelClientName);
	}

	public Set<HostAndPort> sentinels() {
		Set<HostAndPort> set = new LinkedHashSet<>();
		for (String sentinel : sentinels) {
			set.add(HostAndPort.from(sentinel));
		}
		return set;
	}

	public List<String> masterNames() {
		return Collections.unmodifiableList(masterNames);
	}

	public Jedis sentinel(HostAndPort sentinel) {
		Jedis j = null;
		try {
			j = new Jedis(sentinel.getHost(), sentinel.getPort(), (int) sentinelConnectionTimeout.toMillis(), (int) sentinelSoTimeout.toMillis());
			if (sentinelPassword != null) {
				j.auth(sentinelPassword);
			}
			if (sentinelClientName != null) {
				j.clientSetname(sentinelClientName);
			}
			return j;
		} catch (Exception e) {
			if (j != null) {
				j.close();
			}
			throw e;
		}
	}
}
