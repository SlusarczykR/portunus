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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class PortunusBenchmark {

    private static final Logger log = LoggerFactory.getLogger(PortunusBenchmark.class);

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
        IntStream.rangeClosed(1, 10)
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
    public void getCache(Blackhole bh) {
        log.info("Cleaning up global benchmark state");
        Cache<String, String> cache = getTestCache();
        bh.consume(cache);
    }

    @Benchmark
    public void getCacheEntry(Blackhole bh) {
        cache.getEntry("testKey1").ifPresent(bh::consume);
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
