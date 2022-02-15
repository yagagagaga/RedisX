package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.util.Log;

public class JedisSentinelPools implements Closeable, Log {

	private final Set<MasterListener> masterListeners = new HashSet<>();
	private final List<JedisSentinelPool> pools = new ArrayList<>();

	private final List<String> masterNames;
	private final Set<String> sentinels;
	private final GenericObjectPoolConfig<?> poolConfig;
	private final int connectionTimeout;
	private final int soTimeout;
	private final int infiniteSoTimeout;
	private final String user;
	private final String password;
	private final int database;
	private final String clientName;
	private final int sentinelConnectionTimeout;
	private final int sentinelSoTimeout;
	private final String sentinelUser;
	private final String sentinelPassword;
	private final String sentinelClientName;

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			int infiniteSoTimeout,
			String user,
			String password,
			int database,
			String clientName,
			int sentinelConnectionTimeout,
			int sentinelSoTimeout,
			String sentinelUser,
			String sentinelPassword,
			String sentinelClientName) {
		this.masterNames = masterNames;
		this.sentinels = sentinels;
		this.poolConfig = poolConfig;
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.infiniteSoTimeout = infiniteSoTimeout;
		this.user = user;
		this.password = password;
		this.database = database;
		this.clientName = clientName;
		this.sentinelConnectionTimeout = sentinelConnectionTimeout;
		this.sentinelSoTimeout = sentinelSoTimeout;
		this.sentinelUser = sentinelUser;
		this.sentinelPassword = sentinelPassword;
		this.sentinelClientName = sentinelClientName;
		// 初始化
		initPools();
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			int infiniteSoTimeout,
			String user,
			String password,
			int database,
			String clientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				infiniteSoTimeout,
				user,
				password,
				database,
				clientName,
				Protocol.DEFAULT_TIMEOUT,
				Protocol.DEFAULT_TIMEOUT,
				null,
				null,
				null);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String user,
			String password,
			int database,
			String clientName,
			int sentinelConnectionTimeout,
			int sentinelSoTimeout,
			String sentinelUser,
			String sentinelPassword,
			String sentinelClientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				0,
				user,
				password,
				database,
				clientName,
				sentinelConnectionTimeout,
				sentinelSoTimeout,
				sentinelUser,
				sentinelPassword,
				sentinelClientName);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String user,
			String password,
			int database,
			String clientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				user,
				password,
				database,
				clientName,
				Protocol.DEFAULT_TIMEOUT,
				Protocol.DEFAULT_TIMEOUT,
				null,
				null,
				null);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String user,
			String password,
			int database) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				user,
				password,
				database,
				null);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout,
			String password,
			int database) {
		this(masterNames, sentinels, poolConfig, timeout, timeout, null, password, database);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				Protocol.DEFAULT_TIMEOUT,
				null,
				Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelPools(List<String> masterNames,
			Set<String> sentinels) {
		this(
				masterNames,
				sentinels,
				new GenericObjectPoolConfig<>(),
				Protocol.DEFAULT_TIMEOUT,
				null,
				Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout,
			String password) {
		this(masterNames, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelPools(List<String> masterNames,
			Set<String> sentinels, String password) {
		this(
				masterNames,
				sentinels,
				new GenericObjectPoolConfig<>(),
				Protocol.DEFAULT_TIMEOUT,
				password);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String password,
			int database,
			String clientName,
			int sentinelConnectionTimeout,
			int sentinelSoTimeout,
			String sentinelPassword,
			String sentinelClientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				null,
				password,
				database,
				clientName,
				sentinelConnectionTimeout,
				sentinelSoTimeout,
				null,
				sentinelPassword,
				sentinelClientName);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			String password,
			String sentinelPassword) {
		this(
				masterNames,
				sentinels,
				new GenericObjectPoolConfig<>(),
				Protocol.DEFAULT_TIMEOUT,
				Protocol.DEFAULT_TIMEOUT,
				password,
				Protocol.DEFAULT_DATABASE,
				null,
				Protocol.DEFAULT_TIMEOUT,
				Protocol.DEFAULT_TIMEOUT,
				sentinelPassword,
				null);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout) {
		this(masterNames, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			String password) {
		this(masterNames, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout,
			String user,
			String password,
			int database) {
		this(masterNames, sentinels, poolConfig, timeout, timeout, user, password, database);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout,
			String user,
			String password,
			int database,
			String clientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				timeout,
				timeout,
				user,
				password,
				database,
				clientName);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String password,
			int database) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				null,
				password,
				database,
				null);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int connectionTimeout,
			int soTimeout,
			String password,
			int database,
			String clientName) {
		this(
				masterNames,
				sentinels,
				poolConfig,
				connectionTimeout,
				soTimeout,
				null,
				password,
				database,
				clientName);
	}

	public JedisSentinelPools(
			List<String> masterNames,
			Set<String> sentinels,
			GenericObjectPoolConfig<?> poolConfig,
			int timeout,
			String password,
			int database,
			String clientName) {
		this(masterNames, sentinels, poolConfig, timeout, timeout, password, database, clientName);
	}

	public void initPools() {
		info("begin init jedis sentinel pools......");
		Map<String, List<JedisSentinelPool>> masters = new java.util.LinkedHashMap<>();
		for (String masterName : masterNames) {
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(
					masterName,
					iterableOnce(sentinels),
					poolConfig,
					connectionTimeout,
					soTimeout,
					infiniteSoTimeout,
					user,
					password,
					database,
					clientName,
					sentinelConnectionTimeout,
					sentinelSoTimeout,
					sentinelUser,
					sentinelPassword,
					sentinelClientName);
			pools.add(jedisSentinelPool);
			masters
					.computeIfAbsent(masterName, k -> new ArrayList<>())
					.add(jedisSentinelPool);
		}

		for (String sentinel : sentinels) {
			HostAndPort hap = HostAndPort.parseString(sentinel);
			MasterListener masterListener = new MasterListener(
					masters,
					hap.getHost(),
					hap.getPort(),
					sentinelConnectionTimeout,
					sentinelSoTimeout,
					sentinelUser,
					sentinelPassword,
					sentinelClientName);
			masterListener.setDaemon(true);
			masterListeners.add(masterListener);
			masterListener.start();
		}
	}

// 传给ShardedJedis的是现成的Jedis
	public List<JedisShardInfo> getShards() {

		List<PrebuiltJedisShardInfo> shards = new ArrayList<>();

		try {
			for (JedisSentinelPool pool : pools) {
				shards.add(new PrebuiltJedisShardInfo(pool.getResource()));
			}
		} catch (Exception ex) {
			closeAll(shards);
			throw ex;
		}
		return shards.stream().map(a -> (JedisShardInfo) a).collect(Collectors.toList());
	}

	public void close() {
		masterListeners.forEach(MasterListener::shutdown);
		closeAll(pools);
	}

	public void closeAll(Iterable<? extends Closeable> closeables) {
		closeables.forEach(a -> {
			try {
				a.close();
			} catch (IOException ignored) {
			}
		});
	}

	public Set<String> iterableOnce(Set<String> original) {
		return new HashSet<String>() {

			final LinkedHashSet<String> copy = new LinkedHashSet<>();

			{
				copy.addAll(original);
			}

			@Override
			public Iterator<String> iterator() {
				final Iterator<String> iterator = new LinkedHashSet<>(copy).iterator();
				copy.clear();
				return iterator;
			}

			@Override
			public String toString() {
				return copy.toString();
			}
		};
	}
}
