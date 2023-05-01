package org.slusarczykr.portunus.cache.cluster.event.publisher;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent.ClusterEventType;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberJoinedEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        int multicastPort = clusterService.getClusterConfig().getMulticastPort();
        log.info("Initializing multicast publisher for port: {}", multicastPort);
        this.multicastPublisher = new MulticastPublisher(multicastPort);
    }

    @Override
    public void publishEvent(ClusterEvent event) {
        execute(() -> {
            if (isMulticastEvent(event)) {
                sendMulticastEvent(event.getMemberJoinedEvent());
            } else {
                sendEventToMembers(event);
            }
        });
    }

    private boolean isMulticastEvent(ClusterEvent event) {
        return event.getEventType() == ClusterEventType.MemberJoinedEvent
                && clusterService.getClusterConfig().isMulticast();
    }

    @SneakyThrows
    private void sendMulticastEvent(MemberJoinedEvent event) {
        log.info("Sending '{}' to multicast channel", ClusterEventType.MemberJoinedEvent);
        AddressDTO address = event.getAddress();
        String message = String.format("%s:%d", address.getHostname(), address.getPort());
        multicastPublisher.publish(message);
    }

    private void sendEventToMembers(ClusterEvent event) {
        log.info("Sending '{}' to remote cluster members", event.getEventType());
        List<RemotePortunusServer> remoteServers = clusterService.getDiscoveryService().remoteServers();

        if (!remoteServers.isEmpty()) {
            remoteServers.forEach(it -> sendEvent(it, event));
        } else {
            log.info("No remote cluster members registered");
        }
    }

    private static void sendEvent(RemotePortunusServer server, ClusterEvent event) {
        try {
            log.info("Sending '{}' to '{}'", event.getEventType(), server.getPlainAddress());
            server.sendEvent(event);
        } catch (Exception e) {
            log.error("Could not sent '{}' to '{}'", event.getEventType(), server.getPlainAddress());
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
