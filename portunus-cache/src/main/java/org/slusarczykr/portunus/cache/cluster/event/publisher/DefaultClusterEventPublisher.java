package org.slusarczykr.portunus.cache.cluster.event.publisher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.slusarczykr.portunus.cache.cluster.event.MulticastConstants.HOST;
import static org.slusarczykr.portunus.cache.cluster.event.MulticastConstants.MESSAGE_MARKER;

public class DefaultClusterEventPublisher extends AbstractAsyncService implements ClusterEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterEventPublisher.class);

    private final MulticastPublisher multicastPublisher;

    public static DefaultClusterEventPublisher newInstance(ClusterService clusterService) {
        return new DefaultClusterEventPublisher(clusterService);
    }

    public DefaultClusterEventPublisher(ClusterService clusterService) {
        super(clusterService);
        int multicastPort = clusterService.getClusterConfig().getMulticast().getPort();
        log.info("Initializing multicast publisher for port {}", multicastPort);
        this.multicastPublisher = new MulticastPublisher(multicastPort);
    }

    @Override
    public void publishEvent(ClusterEvent event) {
        execute(() -> {
            if (isMulticastEnabled()) {
                sendMulticastEvent(event);
            } else {
                log.debug("Sending '{}' to remote cluster members", event.getEventType());
                withClusterMembers(it -> {
                    log.debug("Sending '{}' to '{}'", event.getEventType(), it.getPlainAddress());
                    it.sendEvent(event);
                });
            }
        });
    }

    @Override
    public void publishEvent(PartitionEvent event) {
        log.debug("Sending '{}' [{}] to remote cluster members", event.getEventType(), event.getPartitionId());
        withClusterMembers(it -> {
            log.debug("Sending '{}' [{}] to '{}'", event.getEventType(), event.getPartitionId(), it.getPlainAddress());
            it.sendEvent(event);
        });
    }

    private boolean isMulticastEnabled() {
        return clusterService.getClusterConfig().getMulticast().isEnabled();
    }

    @SneakyThrows
    private void sendMulticastEvent(ClusterEvent event) {
        log.debug("Sending '{}' to multicast channel", event.getEventType());
        AddressDTO address = event.getFrom();
        String message = String.format("%s:%d#%s", address.getHostname(), address.getPort(), event.getEventType().name());
        multicastPublisher.publish(message);
    }

    private void withClusterMembers(Consumer<RemotePortunusServer> operation) {
        List<RemotePortunusServer> remoteServers = clusterService.getDiscoveryService().remoteServers();

        if (!remoteServers.isEmpty()) {
            remoteServers.forEach(it -> sendEvent(it, operation));
        } else {
            log.debug("No remote cluster members registered");
        }
    }

    private static void sendEvent(RemotePortunusServer server, Consumer<RemotePortunusServer> operation) {
        try {
            operation.accept(server);
        } catch (Exception e) {
            log.error("Could not process operation on server: '{}'", server.getPlainAddress());
        }
    }

    @Override
    public ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("event-publisher-%d")
                        .build()
        );
    }

    @Override
    public String getName() {
        return ClusterEventPublisher.class.getSimpleName();
    }

    private record MulticastPublisher(int port) {

        public void publish(String multicastMessage) throws IOException {
            try (DatagramSocket socket = new DatagramSocket()) {
                InetAddress group = InetAddress.getByName(HOST);
                byte[] buf = String.format("%s%s", multicastMessage, MESSAGE_MARKER).getBytes();

                DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);
                socket.send(packet);
            }
        }
    }
}
