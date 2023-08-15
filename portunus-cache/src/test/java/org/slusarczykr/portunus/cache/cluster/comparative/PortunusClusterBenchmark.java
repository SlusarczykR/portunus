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
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class PortunusClusterBenchmark {

    private static final Logger log = LoggerFactory.getLogger(PortunusClusterBenchmark.class);

    private List<PortunusCluster> portunusInstances;
    private Cache<String, String> cache;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PortunusClusterBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        log.info("Setting up global benchmark state");
        portunusInstances = new ArrayList<>();
        IntStream.range(0, 3).forEach(i -> portunusInstances.add(PortunusCluster.newInstance()));
        cache = portunusInstances.get(0).getCache("test");
        IntStream.rangeClosed(1, 10).forEach(i -> cache.put("testKey" + i, "testValue" + i));
    }

    @TearDown
    public void tearDown() {
        log.info("Cleaning up global benchmark state");
        portunusInstances.forEach(it -> {
            try {
                it.shutdown();
            } catch (Exception e) {
            }
        });
    }

    @Setup(Level.Invocation)
    public void setupEach() {
        log.info("Setting up benchmark state");
        cache = getTestMap();
        IntStream.rangeClosed(1, 10)
                .forEach(i -> cache.put("testKey" + i, "testValue" + i));
    }

    @TearDown(Level.Invocation)
    public void tearDownEach() {
        log.info("Cleaning up benchmark state");
        List<String> keys = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> "testKey" + i)
                .toList();
        getTestMap().removeAll(keys);
    }

    private Cache<String, String> getTestMap() {
        return portunusInstances.get(0).getCache("test");
    }

    @Benchmark
    public void getCache(Blackhole bh) {
        log.info("Cleaning up global benchmark state");
        Cache<String, String> cache = portunusInstances.get(0).getCache("test");
        bh.consume(cache);
    }

    @Benchmark
    public void getCacheEntry(Blackhole bh) {
        Cache.Entry<String, String> entry = cache.getEntry("testKey1").get();
        bh.consume(entry);
    }

    @Benchmark
    public void getCacheEntries(Blackhole bh) {
        Collection<Cache.Entry<String, String>> entries = cache.getEntries(List.of("testKey1", "testKey3"));
        bh.consume(entries);
    }

    @Benchmark
    public void putCacheEntry(Blackhole bh) {
        cache.put("testKey11", "testValue11");
        bh.consume(cache);
    }

    @Benchmark
    public void removeCacheEntry(Blackhole bh) {
        Cache.Entry<String, String> entry = cache.remove("testKey3");
        bh.consume(entry);
    }
}
