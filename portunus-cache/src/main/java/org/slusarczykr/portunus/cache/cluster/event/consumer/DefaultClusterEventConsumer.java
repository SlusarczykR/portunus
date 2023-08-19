package org.slusarczykr.portunus.cache.cluster.event.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionChangeDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.*;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent.ClusterEventType;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.slusarczykr.portunus.cache.cluster.event.MulticastConstants.HOST;
import static org.slusarczykr.portunus.cache.cluster.event.MulticastConstants.MESSAGE_MARKER;

public class DefaultClusterEventConsumer extends AbstractAsyncService implements ClusterEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterEventConsumer.class);

    public static DefaultClusterEventConsumer newInstance(ClusterService clusterService) {
        return new DefaultClusterEventConsumer(clusterService);
    }

    public DefaultClusterEventConsumer(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void onInitialization() throws PortunusException {
        if (clusterService.getClusterConfig().getMulticast().isEnabled()) {
            int multicastPort = clusterService.getClusterConfig().getMulticast().getPort();
            log.info("Initializing multicast receiver on port {}", multicastPort);
            new MulticastReceiver(multicastPort, this::handleMulticastClusterEvent).start();
            log.info("Multicast receiver was started");
        } else {
            log.info("Multicast is not enabled. Multicast receiver will not be started");
        }
    }

    private void handleMulticastClusterEvent(ClusterEvent event) {
        Address address = clusterService.getConversionService().convert(event.getFrom());
        try {
            if (!isLocalAddress(address)) {
                handleClusterEvent(event);
            } else {
                log.debug("Skipping self {} event", event.getEventType());
            }
        } catch (Exception e) {
            log.error("Could not process multicast cluster '{}' event sent from: '{}'", event.getEventType(), address, e);
        }
    }

    private boolean isLocalAddress(Address address) {
        return address.equals(clusterService.getClusterConfigService().getLocalServerAddress());
    }

    @Override
    public void consumeEvent(ClusterEvent event) {
        execute(() -> {
            try {
                log.debug("Received '{}' event", event.getEventType());
                handleClusterEvent(event);
            } catch (Exception e) {
                log.error("Could not process event: {}", event, e);
            }
        });
    }

    private void handleClusterEvent(ClusterEvent event) {
        switch (event.getEventType()) {
            case MemberJoinedEvent -> handleMemberJoinedEvent(event.getMemberJoinedEvent());
            case MemberLeftEvent -> handleMemberLeftEvent(event.getMemberLeftEvent());
            default -> log.error("Unknown cluster event type");
        }
    }

    @SneakyThrows
    private void handleMemberJoinedEvent(MemberJoinedEvent event) {
        Address address = clusterService.getConversionService().convert(event.getAddress());
        ClusterMemberContext context = new ClusterMemberContext(address);
        RemotePortunusServer remoteServer = RemotePortunusServer.newInstance(clusterService, context);

        boolean registered = clusterService.getDiscoveryService().register(remoteServer);

        if (registered) {
            remoteServer.register();
        }
    }

    @SneakyThrows
    private void handleMemberLeftEvent(MemberLeftEvent event) {
        Address address = clusterService.getConversionService().convert(event.getAddress());
        shutdownServer(address);
        clusterService.getDiscoveryService().unregister(address);
    }

    private void shutdownServer(Address address) {
        try {
            RemotePortunusServer remoteServer = (RemotePortunusServer) clusterService.getDiscoveryService().getServer(address, true);
            remoteServer.shutdown();
        } catch (Exception e) {
            log.error("Could not shutdown remote server with address: '{}'", address);
        }
    }

    @Override
    public void consumeEvent(PartitionEvent event) {
        execute(() -> {
            try {
                log.debug("Received '{}' [{}] event", event.getEventType(), event.getPartitionId());
                handlePartitionEvent(event);
            } catch (Exception e) {
                log.error("Could not process event: {}", event, e);
            }
        });
    }

    private void handlePartitionEvent(PartitionEvent event) {
        switch (event.getEventType()) {
            case PartitionCreated -> handlePartitionCreatedEvent(event.getPartitionCreatedEvent());
            case PartitionUpdated -> handlePartitionUpdatedEvent(event.getPartitionUpdatedEvent());
            default -> log.error("Unknown event type");
        }
    }

    @SneakyThrows
    private void handlePartitionCreatedEvent(PartitionCreatedEvent event) {
        processPartitionChange(event.getPartitionChange());
    }

    @SneakyThrows
    private void handlePartitionUpdatedEvent(PartitionUpdatedEvent event) {
        processPartitionChange(event.getPartitionChange());
    }

    private <K extends Serializable, V extends Serializable> void processPartitionChange(PartitionChangeDTO partitionChangeDTO) {
        Partition.Change<K, V> partitionChange = clusterService.getConversionService().convert(partitionChangeDTO);
        log.debug("Received partition change for: {}", partitionChange.getPartition());

        clusterService.getPartitionService().register(partitionChange.getPartition());
        updateLocalCachesIfPartitionOwner(partitionChange);
    }

    private <K extends Serializable, V extends Serializable> void updateLocalCachesIfPartitionOwner(Partition.Change<K, V> partitionChange) {
        if (isPartitionReplicaOwner(partitionChange.getPartition())) {
            clusterService.getLocalServer().update(partitionChange);
        }
    }

    private boolean isPartitionReplicaOwner(Partition partition) {
        int partitionId = partition.getPartitionId();
        return clusterService.getReplicaService().isPartitionReplicaOwner(partitionId);
    }

    @Override
    public ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("event-consumer-%d")
                        .build()
        );
    }

    @Override
    public String getName() {
        return ClusterEventConsumer.class.getSimpleName();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    private class MulticastReceiver extends Thread {

        private final int port;
        private final Consumer<ClusterEvent> operation;

        public MulticastReceiver(int port, Consumer<ClusterEvent> operation) {
            this.port = port;
            this.operation = operation;
        }

        @Override
        public void run() {
            try {
                listenForMulticast();
            } catch (IOException e) {
                throw new FatalPortunusException("Unable to start multicast receiver", e);
            }
        }

        private void listenForMulticast() throws IOException {
            try (MulticastSocket socket = new MulticastSocket(port)) {
                byte[] buf = new byte[256];
                InetAddress group = InetAddress.getByName(HOST);
                socket.joinGroup(group);

                while (!clusterService.getPortunusClusterInstance().isShutdown()) {
                    String received = receive(socket, buf);

                    if (received.endsWith(MESSAGE_MARKER)) {
                        log.debug("Received multicast message: {}", received);
                        Address address = parseAddress(received);
                        boolean left = received.contains(ClusterEventType.MemberLeftEvent.name());

                        operation.accept(createClusterEvent(address, left));
                    }
                }
            }
        }

        private ClusterEvent createClusterEvent(Address address, boolean left) {
            if (left) {
                return createMemberLeftEvent(address);
            }
            return createMemberJoinedEvent(address);
        }

        private static Address parseAddress(String source) {
            String addressToEventType = source.split("@")[0];
            String address = addressToEventType.split("#")[0];

            return Address.from(address);
        }

        private String receive(MulticastSocket socket, byte[] buf) throws IOException {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);

            return new String(packet.getData(), 0, packet.getLength());
        }

        private ClusterEvent createMemberJoinedEvent(Address address) {
            return ClusterEvent.newBuilder()
                    .setFrom(toAddressDTO(address))
                    .setEventType(ClusterEventType.MemberJoinedEvent)
                    .setMemberJoinedEvent(
                            MemberJoinedEvent.newBuilder()
                                    .setAddress(toAddressDTO(address))
                                    .build()
                    ).build();
        }

        private ClusterEvent createMemberLeftEvent(Address address) {
            return ClusterEvent.newBuilder()
                    .setFrom(toAddressDTO(address))
                    .setEventType(ClusterEventType.MemberLeftEvent)
                    .setMemberLeftEvent(
                            MemberLeftEvent.newBuilder()
                                    .setAddress(toAddressDTO(address))
                                    .build()
                    ).build();
        }

        private static AddressDTO toAddressDTO(Address address) {
            return AddressDTO.newBuilder()
                    .setHostname(address.hostname())
                    .setPort(address.port())
                    .build();
        }
    }
}
