package redis.clients.jedis.client;

import redis.clients.jedis.JedisSentinelPools;
import redis.clients.jedis.ShardedJedisSentinel;
import redis.clients.jedis.*;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class JedisCmdExecutor {

    private final Set<Method> methods;

    private final int scanParallelism;
    private final int flushParallelism;
    private final int flushBatch;

    private final JedisClientConfig jedisClientConfig;
    private final JedisSentinelPools pools;

    public JedisCmdExecutor(JedisClientConfig jedisClientConfig) {
        this.jedisClientConfig = jedisClientConfig;
        pools = jedisClientConfig.jedisSentinelPools();
        
        methods = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ShardedJedis.class.getDeclaredMethods())));
        scanParallelism = jedisClientConfig.prop.getInteger("scan.parallelism", 10);
        flushParallelism = jedisClientConfig.prop.getInteger("flush.parallelism", 10);
        flushBatch = jedisClientConfig.prop.getInteger("flush.batch", 10);
    }

    public Set<String> commands() {
        Set<String> set = methods.stream().map(Method::getName).collect(Collectors.toSet());
        set.add("sentinels");
        set.add("masters");
        set.add("dbSize");
        set.add("scan");
        set.add("flushDB");
        return set;
    }

    public void run(String[] words, PrintWriter writer) throws Throwable {
        String cmd = words[0];
        LinkedList<String> params = new LinkedList<>(Arrays.asList(words));
        params.removeFirst();
        if ("sentinels".equalsIgnoreCase(cmd) && params.size() == 0) {
            doSentinels(writer);
            return;
        }
        if ("masters".equalsIgnoreCase(cmd) && params.size() == 0) {
            doMasters(writer);
            return;
        }
        if ("dbSize".equalsIgnoreCase(cmd) && params.size() == 0) {
            doDbSize(writer);
            return;
        }
        if ("scan".equalsIgnoreCase(cmd) && params.size() == 2) {
            doScan(params.get(0), Integer.parseInt(params.get(1)), writer);
            return;
        }
        if ("flushDB".equalsIgnoreCase(cmd) && params.size() == 1) {
            if ("Sure".equals(params.get(0))) {
                doFlushDB(writer);
                return;
            }
        }
        final Method method = chooseBest(cmd, params.size());
        if (method == null) {
            throw new IllegalStateException("No such CMD: '" + cmd + "' with " + params.size() + " args");
        }
        Class<?>[] pTypes = method.getParameterTypes();
        final Object[] args = new Object[pTypes.length];
        for (int i = 0; i < pTypes.length; i++) {
            if (pTypes[i].isArray()) {
                if (i != pTypes.length - 1 || !pTypes[i].getComponentType().isAssignableFrom(String.class)) {
                    throw new IllegalStateException("Unsupported method: " + method);
                }
                String[] arr = new String[params.size() - i];
                for (int j = 0; j < arr.length; j++) {
                    arr[j] = params.get(i + j);
                }
                args[i] = arr;
            } else {
                args[i] = resolveValue(params.get(i), pTypes[i]);
            }
        }
        try (ShardedJedis shardedJedis = new ShardedJedisSentinel(jedisSentinelPools().getShards())) {
            formatPrint(method.invoke(shardedJedis, args), writer);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private JedisSentinelPools jedisSentinelPools() {
        return pools;
    }

    private void doSentinels(PrintWriter printWriter) {
        List<String> lst = new LinkedList<>();
        for (HostAndPort sentinel : jedisClientConfig.sentinels()) {
            String str;
            try (Jedis j = jedisClientConfig.sentinel(sentinel)) {
                str = sentinel + " " + j.sentinelMasters().size();
            } catch (RuntimeException e) {
                str = sentinel + " " + e.getMessage();
            }
            lst.add(str);
        }
        formatPrint(lst, printWriter);
    }

    private void doMasters(PrintWriter printWriter) {
        List<Map<String, String>> masters = null;
        for (HostAndPort sentinel : jedisClientConfig.sentinels()) {
            try (Jedis j = jedisClientConfig.sentinel(sentinel)) {
                masters = j.sentinelMasters();
                break;
            } catch (RuntimeException ignored) {

            }
        }
        if (masters == null) {
            throw new IllegalStateException("All sentinels not available");
        }
        Map<String, Map<String, String>> map = new HashMap<>();
        for (Map<String, String> master : masters) {
            map.put(master.get("name"), master);
        }
        List<String> lst = new LinkedList<>();
        for (String masterName : jedisClientConfig.masterNames()) {
            Map<String, String> master = map.get(masterName);
            if (master == null) {
                lst.add(masterName + " --> " + "Not Available");
            } else {
                lst.add(masterName + " --> " + master.get("ip") + ":" + master.get("port") + " " + master.get("num-slaves"));
            }
        }
        formatPrint(lst, printWriter);
    }

    private void doDbSize(PrintWriter printWriter) {
        try (ShardedJedis shardedJedis = new ShardedJedisSentinel(jedisSentinelPools().getShards())) {
            List<String> lst = new LinkedList<>();
            long total = 0;
            for (Jedis j : shardedJedis.getAllShards()) {
                String str;
                try {
                    long size = j.dbSize();
                    total += size;
                    str = resolveServer(j) + " " + size;
                } catch (RuntimeException e) {
                    str = resolveServer(j) + " " + e.getMessage();
                }
                lst.add(str);
            }
            formatPrint(lst, printWriter);
            printWriter.println("Total " + total);
            printWriter.flush();
        }
    }

    private void doScan(String pattern, int limit, PrintWriter writer) throws InterruptedException {
        ScanParams scanParams = new ScanParams().match(pattern).count(Math.min(Math.max(1000, limit), 10000));
        ExecutorService executorService = Executors.newWorkStealingPool(scanParallelism);
        try (ShardedJedis shardedJedis = new ShardedJedisSentinel(jedisSentinelPools().getShards())) {
            List<String> err = new LinkedList<>();
            int seq = 0;
            CompletionService<ScanResult<String>> cs = new ExecutorCompletionService<>(executorService);
            Map<Future<ScanResult<String>>, Jedis> scanners = new HashMap<>();
            long start = System.currentTimeMillis();
            for (Jedis j : shardedJedis.getAllShards()) {
                scanners.put(cs.submit(() -> j.scan("0", scanParams)), j);
            }
            while (!scanners.isEmpty() && seq < limit) {
                Future<ScanResult<String>> future = cs.take();
                Jedis j = scanners.remove(future);
                ScanResult<String> scanResult;
                try {
                    scanResult = future.get();
                } catch (ExecutionException ee) {
                    err.add(resolveServer(j) + " " + ee.getCause().getMessage());
                    continue;
                }
                for (String key : scanResult.getResult()) {
                    if (seq >= limit) {
                        break;
                    }
                    writer.print("(" + (++seq) + ") ");
                    writer.println(key);
                }
                writer.flush();
                String cursor = scanResult.getCursor();
                if (seq < limit && !"0".equals(cursor)) {
                    scanners.put(cs.submit(() -> j.scan(cursor, scanParams)), j);
                }
            }
            long end = System.currentTimeMillis();
            writer.println("Found " + seq + " in " + formatDuration(end - start));
            writer.flush();
            if (!err.isEmpty()) {
                writer.println("Error " + err.size());
                writer.flush();
                formatPrint(err, writer);
            }
            if (!scanners.isEmpty()) {
                writer.println("Waiting " + scanners.size() + " scanners to shutdown... ");
                writer.flush();
                while (!scanners.isEmpty()) {
                    scanners.remove(cs.take());
                }
                writer.println("Done");
                writer.flush();
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private void doFlushDB(PrintWriter writer) throws InterruptedException, ExecutionException {
        ScanParams scanParams = new ScanParams().match("*").count(Math.min(Math.max(1000, flushBatch), 10000));
        ExecutorService executorService = Executors.newWorkStealingPool(flushParallelism);
        try (ShardedJedis shardedJedis = new ShardedJedisSentinel(jedisSentinelPools().getShards())) {
            writer.println("WARNING: flushDB(" + shardedJedis.getAllShards().size() + ") in progress...");
            writer.flush();
            AtomicLong cnt = new AtomicLong();
            AtomicLong total = new AtomicLong();
            CompletionService<String> cs = new ExecutorCompletionService<>(executorService);
            long start = System.currentTimeMillis();
            for (Jedis j : shardedJedis.getAllShards()) {
                cs.submit(() -> {
                    String str;
                    try {
                        long sum = 0;
                        int err = 0;
                        String cursor = "0";
                        do {
                            ScanResult<String> scanResult = j.scan(cursor, scanParams);
                            if (!scanResult.getResult().isEmpty()) {
                                total.addAndGet(scanResult.getResult().size());
                                try {
                                    long del = j.del(scanResult.getResult().toArray(new String[0]));
                                    sum += del;
                                    cnt.addAndGet(del);
                                } catch (RuntimeException e) {
                                    err++;
                                }
                            }
                            cursor = scanResult.getCursor();
                        } while (!"0".equals(cursor));
                        str = resolveServer(j) + " " + sum + (err > 0 ? " (Error: " + err + " times)" : "");
                    } catch (RuntimeException e) {
                        str = resolveServer(j) + " " + e.getMessage();
                    }
                    return str;
                });
            }
            int seq = 0;
            for (int i = 0; i < shardedJedis.getAllShards().size(); i++) {
                Future<String> future = cs.take();
                writer.print("(" + (++seq) + ") ");
                writer.println(future.get());
                writer.flush();
            }
            long end = System.currentTimeMillis();
            writer.println("Deleted " + cnt.get() + " / " + total.get() + " in " + formatDuration(end - start));
            writer.flush();
        } finally {
            executorService.shutdown();
        }
    }

    private static String formatDuration(long millis) {
        StringBuilder sb = new StringBuilder();
        sb.append(".");
        sb.append(new DecimalFormat("000").format(millis % 1000));
        sb.append("s");
        long seconds = millis / 1000;
        if (seconds < 60) {
            sb.insert(0, seconds);
        } else {
            sb.insert(0, new DecimalFormat("00").format(seconds % 60));
        }
        long minutes = seconds / 60;
        if (minutes > 0) {
            sb.insert(0, ":");
            if (minutes < 60) {
                sb.insert(0, minutes);
            } else {
                sb.insert(0, new DecimalFormat("00").format(minutes % 60));
            }
        }
        long hours = minutes / 60;
        if (hours > 0) {
            sb.insert(0, ":");
            sb.insert(0, hours);
        }
        return sb.toString();
    }

    private Method chooseBest(String cmd, int paramNum) {
        Method candidate = null;
        for (Method method : methods) {
            if (!method.getName().equalsIgnoreCase(cmd)) {
                continue;
            }
            Class<?>[] pTypes = method.getParameterTypes();
            boolean incompatible = false;
            for (Class<?> pType : pTypes) {
                if (Map.class.isAssignableFrom(pType)) {
                    incompatible = true;
                    break;
                }
            }
            if (incompatible) {
                continue;
            }
            if (pTypes.length == paramNum) {
                candidate = method;
                break;
            }
            if (pTypes.length > paramNum) {
                continue;
            }
            if (pTypes[pTypes.length - 1].isArray()) {
                candidate = method;
                break;
            }
        }
        return candidate;
    }

    private static void formatPrint(Object obj, PrintWriter writer) {
        if (obj instanceof Collection) {
            Collection<?> col = (Collection<?>) obj;
            if (col.isEmpty()) {
                writer.println("(EmptyCollection)");
            } else {
                Iterator<?> it = col.iterator();
                int i = 0;
                while (it.hasNext()) {
                    writer.print("(" + (++i) + ") ");
                    writer.println(showElement(it.next()));
                }
            }
        } else if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.isEmpty()) {
                writer.println("(EmptyMap)");
            } else {
                int i = 0;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    writer.print("(" + (++i) + ") ");
                    writer.print(entry.getKey());
                    writer.print(" --> ");
                    writer.println(showElement(entry.getValue()));
                }
            }
        } else {
            writer.println(showElement(obj));
        }
        writer.flush();
    }

    private static String showElement(Object element) {
        if (element == null) {
            return "null";
        }
        if (element instanceof Tuple) {
            Tuple tuple = (Tuple) element;
            DecimalFormat df = new DecimalFormat("#.###");
            return df.format(tuple.getScore()) + ": " + tuple.getElement();
        }
        return element.toString();
    }

    private static Object resolveValue(String value, Class<?> clazz) {
        if (clazz.isAssignableFrom(String.class)) {
            return value;
        }
        if (clazz.isAssignableFrom(Long.class)) {
            return Long.valueOf(value);
        }
        if (clazz.isAssignableFrom(long.class)) {
            return Long.parseLong(value);
        }
        if (clazz.isAssignableFrom(Integer.class)) {
            return Integer.valueOf(value);
        }
        if (clazz.isAssignableFrom(int.class)) {
            return Integer.parseInt(value);
        }
        if (clazz.isAssignableFrom(Double.class)) {
            return Double.valueOf(value);
        }
        if (clazz.isAssignableFrom(double.class)) {
            return Double.parseDouble(value);
        }
        throw new IllegalStateException("Unsupported parameter type: " + clazz);
    }

    private static String resolveServer(Jedis jedis) {
        Client client = jedis.getClient();
        return client.getHost() + ":" + client.getPort();
    }
}
