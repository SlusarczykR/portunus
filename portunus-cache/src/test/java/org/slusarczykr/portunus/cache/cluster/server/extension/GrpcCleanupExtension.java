package org.slusarczykr.portunus.cache.cluster.server.extension;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GrpcCleanupExtension implements AfterEachCallback {

    private final static Logger log = LoggerFactory.getLogger(GrpcCleanupExtension.class);

    public static final int TERMINATION_TIMEOUT_MS = 250;
    public static final int MAX_NUM_TERMINATIONS = 100;

    private final List<CleanupTarget> cleanupTargets = new ArrayList<>();

    public ManagedChannel addService(BindableService service) throws IOException {
        String serverName = InProcessServerBuilder.generateName();

        cleanupTargets.add(new ServerCleanupTarget(createInProcessServer(service, serverName)));

        ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        cleanupTargets.add(new ManagedChannelCleanupTarget(managedChannel));

        return managedChannel;
    }

    private static Server createInProcessServer(BindableService service, String serverName) throws IOException {
        return InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .intercept(new GrpcExceptionHandler())
                .addService(service)
                .build()
                .start();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        cleanupTargets.forEach(cleanupTarget -> {
            try {
                int count = 0;
                cleanupTarget.shutdown();
                do {
                    cleanupTarget.awaitTermination(TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    count++;
                    if (count > MAX_NUM_TERMINATIONS) {
                        log.error("Hit max count: {} trying to shut down the cleanup target: {}", count, cleanupTarget);
                        break;
                    }
                } while (!cleanupTarget.isTerminated());
            } catch (Exception e) {
                log.error("Problem shutting down cleanup target: {}", cleanupTarget, e);
            }
        });

        if (isAllTerminated()) {
            cleanupTargets.clear();
        } else {
            log.error("Not all cleanup targets are terminated");
        }
    }

    boolean isAllTerminated() {
        return cleanupTargets.stream().allMatch(CleanupTarget::isTerminated);
    }
}

interface CleanupTarget {
    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException;

    Boolean isTerminated();
}

class ServerCleanupTarget implements CleanupTarget {

    private final Server server;

    ServerCleanupTarget(Server server) {
        this.server = server;
    }

    @Override
    public void shutdown() {
        server.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return server.awaitTermination(timeout, timeUnit);
    }

    @Override
    public Boolean isTerminated() {
        return server.isTerminated();
    }
}

class ManagedChannelCleanupTarget implements CleanupTarget {

    private final ManagedChannel managedChannel;

    public ManagedChannelCleanupTarget(ManagedChannel managedChannel) {
        this.managedChannel = managedChannel;
    }

    @Override
    public void shutdown() {
        managedChannel.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return managedChannel.awaitTermination(timeout, timeUnit);
    }

    @Override
    public Boolean isTerminated() {
        return managedChannel.isTerminated();
    }
}
