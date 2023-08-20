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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class HazelcastBenchmark {

    private static final Logger log = LoggerFactory.getLogger(HazelcastBenchmark.class);

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
        IntStream.rangeClosed(1, 10)
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
    public void getCache(Blackhole bh) {
        IMap<String, String> cache = getTestMap();
        bh.consume(cache);
    }

    @Benchmark
    public void getCacheEntry(Blackhole bh) {
        String entry = cache.get("testKey1");
        bh.consume(entry);
    }

    @Benchmark
    public void getCacheEntries(Blackhole bh) {
        Map<String, String> entries = cache.getAll(Set.of("testKey1", "testKey3"));
        bh.consume(entries);
    }

    @Benchmark
    public void putCacheEntry(Blackhole bh) {
        cache.put("testKey11", "testValue11");
        bh.consume(cache);
    }

    @Benchmark
    public void removeCacheEntry(Blackhole bh) {
        String entry = cache.remove("testKey1");
        bh.consume(entry);
    }
}
