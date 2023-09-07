package org.slusarczykr.portunus.cache.cluster.comparative;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
public class PortunusBenchmark {

    private static final Logger log = LoggerFactory.getLogger(PortunusBenchmark.class);

    private static final int NUMBER_OF_RECORDS = 500000;

    private PortunusCluster portunusInstance;
    private Cache<String, String> cache;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PortunusBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Invocation)
    public void setupEach() {
        log.info("Setting up benchmark state");
        portunusInstance = PortunusCluster.newInstance();
        cache = getTestCache();
        IntStream.rangeClosed(1, NUMBER_OF_RECORDS)
                .forEach(i -> cache.put("testKey" + i, "testValue" + i));
    }

    @TearDown(Level.Invocation)
    public void tearDownEach() {
        log.info("Cleaning up global benchmark state");
        Optional.ofNullable(portunusInstance).ifPresent(it -> {
            try {
                it.shutdown();
            } catch (Exception e) {
            }
        });
    }

    private Cache<String, String> getTestCache() {
        return portunusInstance.getCache("test");
    }

    @Benchmark
    public void getCacheEntries(Blackhole bh) {
        Set<String> keys = getCacheEntries(1, NUMBER_OF_RECORDS).stream()
                .map(Cache.Entry::getKey)
                .collect(Collectors.toSet());

        Collection<Cache.Entry<String, String>> entries = cache.getEntries(keys);

        bh.consume(cache);
        bh.consume(entries);
    }

    @Benchmark
    public void putCacheEntries(Blackhole bh) {
        Set<Cache.Entry<String, String>> entries = getCacheEntries(NUMBER_OF_RECORDS + 1, NUMBER_OF_RECORDS + NUMBER_OF_RECORDS);

        cache.putAll(entries);

        bh.consume(cache);
        bh.consume(entries);
    }

    @Benchmark
    public void removeCacheEntries(Blackhole bh) {
        Set<String> keys = getCacheEntries(1, NUMBER_OF_RECORDS).stream()
                .map(Cache.Entry::getKey)
                .collect(Collectors.toSet());

        cache.removeAll(keys);

        bh.consume(cache);
        bh.consume(keys);
    }

    private Set<Cache.Entry<String, String>> getCacheEntries(int from, int to) {
        return IntStream.rangeClosed(from, to)
                .mapToObj(i -> (Cache.Entry<String, String>) new DefaultCache.Entry<>("testKey" + i, "testValue" + i))
                .collect(Collectors.toSet());
    }
}
