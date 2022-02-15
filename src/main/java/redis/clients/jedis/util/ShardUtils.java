package redis.clients.jedis.util;

public class ShardUtils {
	public static int shardIndex(String key, int shardNum) {
		int hash = ((int) Hashing.MURMUR_HASH.hash(key)) & Integer.MAX_VALUE;
		return hash % shardNum;
	}
}
