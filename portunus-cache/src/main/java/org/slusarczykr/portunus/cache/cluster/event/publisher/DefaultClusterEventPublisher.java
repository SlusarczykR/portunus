package org.slusarczykr.portunus.cache.cluster.event.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultClusterEventPublisher extends AbstractAsyncService implements ClusterEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterEventPublisher.class);

    private static final DefaultClusterEventPublisher INSTANCE = new DefaultClusterEventPublisher();

    public static DefaultClusterEventPublisher getInstance() {
        return INSTANCE;
    }

    private final ClusterService clusterService;

    public DefaultClusterEventPublisher() {
        this.clusterService = DefaultClusterService.getInstance();
    }

    @Override
    public void publishEvent(ClusterEvent event) {
        execute(() -> {
            List<RemotePortunusServer> remoteServers = clusterService.getDiscoveryService().remoteServers();
            remoteServers.forEach(it -> sendEvent(it, event));
        });
    }

    private static void sendEvent(RemotePortunusServer server, ClusterEvent event) {
        try {
            log.info("Sending event {} to {}", event.getEventType(), server.getPlainAddress());
            server.sendEvent(event);
        } catch (Exception e) {
            log.error("Could not send event {} to {}", event.getEventType(), server.getPlainAddress());
        }
    }

    @Override
    public ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    public String getName() {
        return ClusterEventPublisher.class.getSimpleName();
    }
}
