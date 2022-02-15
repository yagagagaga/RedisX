# ShardedJedis
一个基于 Jedis 封装的、支持操作多个 Redis 哨兵集群的客户端。

使用示例：
```java
public class App {
  public static void main(String[] args) {
    List<JedisShardInfo> shards = ShardedJedisSentinelPoolManager.getshardedJedisPools(
      "redis_cluster.properties").getShards();
    ShardedJedisSentinel shardJedis = new ShardedJedisSentinel(shards);
    shardJedis.set("foo", "bar");
    String value = shardJedis.get("foo");
    System.out.println(value);
  }
}
```

同时提供命令行客户端查询 Redis 多哨兵集群：
```shell script
java -cp ShardedJedis-jar-with-dependencies.jar redis.clients.jedis.cmd.RedisCmdTool config.properties

============Welcome!=============
Type here> set foo bar
OK
Type here> get foo
bar
Type here> 
```
