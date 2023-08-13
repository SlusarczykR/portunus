package org.slusarczykr.portunus.cache.cluster.comparative;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class HazelcastInitializationBenchmark {

    private static final Logger log = LoggerFactory.getLogger(HazelcastInitializationBenchmark.class);

    private HazelcastInstance hazelcastInstance;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HazelcastInitializationBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        log.info("Cleaning up the test state");
        Optional.ofNullable(hazelcastInstance).ifPresent(it -> {
            try {
                it.shutdown();
            } catch (Exception e) {
            }
        });

    }

    @Benchmark
    public void initializeClusterInstance(Blackhole bh) {
        log.info("Running 'initializeClusterInstance' benchmark");
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        bh.consume(hazelcastInstance);
    }
}
