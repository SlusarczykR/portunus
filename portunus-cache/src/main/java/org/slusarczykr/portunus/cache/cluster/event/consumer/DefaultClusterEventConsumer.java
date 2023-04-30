package org.slusarczykr.portunus.cache.cluster.event.consumer;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent.ClusterEventType;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberJoinedEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberLeftEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractAsyncService;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

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
        if (clusterService.getClusterConfig().isMulticast()) {
            log.info("Starting multicast receiver");
            int multicastPort = clusterService.getClusterConfig().getMulticastPort();
            new MulticastReceiver(multicastPort, this::handleMulticastEvent).start();
            log.info("Multicast receiver was started");
        } else {
            log.info("Multicast is not enabled. Multicast receiver will not be started");
        }
    }

    private void handleMulticastEvent(MemberJoinedEvent event) {
        Address address = clusterService.getConversionService().convert(event.getAddress());

        if (!address.equals(clusterService.getPortunusClusterInstance().getAddress())) {
            handleEvent(event);
        } else {
            log.info("Skipping self {} event", ClusterEventType.MemberJoinedEvent);
        }
    }

    @Override
    public void consumeEvent(ClusterEvent event) {
        execute(() -> {
            try {
                log.info("Received '{}' payload: {}", event.getEventType(), event);
                handleEvent(event);
            } catch (Exception e) {
                log.error("Could not process event: {}", event, e);
            }
        });
    }

    private void handleEvent(ClusterEvent event) {
        switch (event.getEventType()) {
            case MemberJoinedEvent -> handleEvent(event.getMemberJoinedEvent());
            case MemberLeftEvent -> handleEvent(event.getMemberLeftEvent());
            default -> log.error("Unknown event type");
        }
    }

    @SneakyThrows
    private void handleEvent(MemberJoinedEvent event) {
        Address address = clusterService.getConversionService().convert(event.getAddress());
        int numberOfClusterMembers = clusterService.getClusterConfigService().getNumberOfClusterMembers();
        ClusterMemberContext context = new ClusterMemberContext(address, numberOfClusterMembers);
        RemotePortunusServer portunusServer = RemotePortunusServer.newInstance(clusterService, context);
        clusterService.getDiscoveryService().register(portunusServer);

    }

    @SneakyThrows
    private void handleEvent(MemberLeftEvent event) {
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

    private static class MulticastReceiver extends Thread {

        private static final String MESSAGE_MARKER = "@portunus";
        private static final String HOST = "230.0.0.0";

        private final int port;
        private final Consumer<MemberJoinedEvent> operation;

        public MulticastReceiver(int port, Consumer<MemberJoinedEvent> operation) {
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
            byte[] buf = new byte[256];
            MulticastSocket socket = new MulticastSocket(port);
            InetAddress group = InetAddress.getByName(HOST);
            socket.joinGroup(group);

            while (true) {
                String received = receive(socket, buf);

                if (received.endsWith(MESSAGE_MARKER)) {
                    log.info("Received multicast message: {}", received);
                    Address address = parseAddress(received);
                    operation.accept(createMemberJoinedEvent(address));
                    break;
                }
            }
            socket.leaveGroup(group);
            socket.close();
        }

        private static Address parseAddress(String source) {
            String address = source.split("@")[0];
            return Address.from(address);
        }

        private String receive(MulticastSocket socket, byte[] buf) throws IOException {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);

            return new String(packet.getData(), 0, packet.getLength());
        }

        private MemberJoinedEvent createMemberJoinedEvent(Address address) {
            return MemberJoinedEvent.newBuilder()
                    .setAddress(toAddressDTO(address))
                    .build();
        }

        private static AddressDTO toAddressDTO(Address address) {
            return AddressDTO.newBuilder()
                    .setHostname(address.hostname())
                    .setPort(address.port())
                    .build();
        }
    }
}
