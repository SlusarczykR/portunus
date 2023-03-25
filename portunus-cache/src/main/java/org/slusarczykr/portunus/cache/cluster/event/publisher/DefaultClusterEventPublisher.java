package org.slusarczykr.portunus.cache.cluster.event.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.PortunusClusterInstance;

public class DefaultClusterEventPublisher implements ClusterEventPublisher {

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
        clusterService.getDiscoveryService().remoteServers().forEach(it -> {
            log.info("Sending event {} to {}", event.getEventType(), it.getPlainAddress());
            it.sendEvent(event);
        });
    }

    @Override
    public String getName() {
        return ClusterEventPublisher.class.getSimpleName();
    }
}
