package org.slusarczykr.portunus.cache.cluster.event.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberJoinedEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberLeftEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultClusterEventConsumer extends AbstractAsyncService implements ClusterEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterEventConsumer.class);

    private static final DefaultClusterEventConsumer INSTANCE = new DefaultClusterEventConsumer();

    public static DefaultClusterEventConsumer getInstance() {
        return INSTANCE;
    }

    private final ClusterService clusterService;

    public DefaultClusterEventConsumer() {
        this.clusterService = DefaultClusterService.getInstance();
    }

    @Override
    public void consumeEvent(ClusterEvent event) {
        execute(() -> {
            try {
                handleEvent(event);
            } catch (Exception e) {
                log.error("Could not process event: {}", event, e);
            }
        });
    }

    private void handleEvent(ClusterEvent event) throws PortunusException {
        switch (event.getEventType()) {
            case MemberJoinedEvent -> handleEvent(event.getMemberJoinedEvent());
            case MemberLeftEvent -> handleEvent(event.getMemberLeftEvent());
            default -> log.error("Unknown event type");
        }
    }

    private void handleEvent(MemberJoinedEvent event) throws PortunusException {
        Address address = clusterService.getConversionService().convert(event.getAddress());
        RemotePortunusServer portunusServer = RemotePortunusServer.newInstance(address);
        clusterService.getDiscoveryService().register(portunusServer);

    }

    private void handleEvent(MemberLeftEvent event) throws PortunusException {
        Address address = clusterService.getConversionService().convert(event.getAddress());
        clusterService.getDiscoveryService().unregister(address);
    }

    @Override
    public ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    public String getName() {
        return ClusterEventConsumer.class.getSimpleName();
    }
}
