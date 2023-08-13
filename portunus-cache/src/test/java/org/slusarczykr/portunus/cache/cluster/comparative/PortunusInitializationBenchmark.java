package org.slusarczykr.portunus.cache.cluster.comparative;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class PortunusInitializationBenchmark {

    private static final Logger log = LoggerFactory.getLogger(PortunusInitializationBenchmark.class);

    private PortunusCluster portunusCluster;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PortunusInitializationBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        log.info("Cleaning up the test state");
        Optional.ofNullable(portunusCluster).ifPresent(it -> {
            try {
                it.shutdown();
            } catch (Exception e) {
            }
        });

    }

    @Benchmark
    public void initializeClusterInstance(Blackhole bh) {
        log.info("Running 'initializeClusterInstance' benchmark");
        portunusCluster = PortunusCluster.newInstance();
        bh.consume(portunusCluster);
    }
}
