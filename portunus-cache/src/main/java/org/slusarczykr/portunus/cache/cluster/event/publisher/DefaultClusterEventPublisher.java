package org.slusarczykr.portunus.cache.cluster.event.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultClusterEventPublisher extends AbstractAsyncService implements ClusterEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterEventPublisher.class);

    public static DefaultClusterEventPublisher newInstance(ClusterService clusterService) {
        return new DefaultClusterEventPublisher(clusterService);
    }

    public DefaultClusterEventPublisher(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void publishEvent(ClusterEvent event) {
        execute(() -> {
            log.info("Sending '{}' to remote cluster members", event.getEventType());
            List<RemotePortunusServer> remoteServers = clusterService.getDiscoveryService().remoteServers();

            if (!remoteServers.isEmpty()) {
                remoteServers.forEach(it -> sendEvent(it, event));
            } else {
                log.info("No remote cluster members registered");
            }
        });
    }

    private static void sendEvent(RemotePortunusServer server, ClusterEvent event) {
        try {
            log.info("Sending '{}' to '{}'", event.getEventType(), server.getPlainAddress());
            server.sendEvent(event);
        } catch (Exception e) {
            log.error("Could not send '{}' to '{}'", event.getEventType(), server.getPlainAddress(), e);
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
