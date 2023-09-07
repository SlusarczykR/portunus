package org.slusarczykr.portunus.cache.cluster.comparative;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
public class HazelcastBenchmark {

    private static final Logger log = LoggerFactory.getLogger(HazelcastBenchmark.class);

    private static final int NUMBER_OF_RECORDS = 500000;

    private HazelcastInstance hazelcastInstance;
    private IMap<String, String> cache;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HazelcastBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Invocation)
    public void setupEach() {
        log.info("Setting up benchmark state");
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        cache = getTestMap();
        IntStream.rangeClosed(1, NUMBER_OF_RECORDS)
                .forEach(i -> cache.put("testKey" + i, "testValue" + i));
    }

    @TearDown(Level.Invocation)
    public void tearDownEach() {
        log.info("Cleaning up benchmark state");
        Optional.ofNullable(hazelcastInstance).ifPresent(it -> {
            try {
                it.shutdown();
            } catch (Exception e) {
            }
        });
    }

    private IMap<String, String> getTestMap() {
        return hazelcastInstance.getMap("test");
    }

    @Benchmark
    public void getCacheEntries(Blackhole bh) {
        Set<String> keys = getCacheEntries(1, NUMBER_OF_RECORDS).keySet();
        Map<String, String> entries = cache.getAll(keys);
        bh.consume(cache);
        bh.consume(entries);
    }

    @Benchmark
    public void putCacheEntries(Blackhole bh) {
        Map<String, String> cacheEntries = getCacheEntries(NUMBER_OF_RECORDS + 1, NUMBER_OF_RECORDS + NUMBER_OF_RECORDS);
        cache.putAll(cacheEntries);
        bh.consume(cache);
    }

    @Benchmark
    public void removeCacheEntries(Blackhole bh) {
        Set<String> keys = getCacheEntries(1, NUMBER_OF_RECORDS).keySet();
        cache.removeAll(it -> keys.contains(it.getKey()));
        bh.consume(cache);
    }

    private Map<String, String> getCacheEntries(int from, int to) {
        return IntStream.rangeClosed(from, to)
                .mapToObj(i -> Map.entry("testKey" + i, "testValue" + i))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
