package org.slusarczykr.portunus.cache.cluster.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractClient extends AbstractManaged {

    private final ClusterService clusterService;
    private final Address address;
    private final ManagedChannel channel;

    protected AbstractClient(ClusterService clusterService, Address address) {
        super(clusterService.getManagedService());
        this.clusterService = clusterService;
        this.address = address;
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    protected AbstractClient(ClusterService clusterService, Address address, ManagedChannel channel) {
        super(clusterService.getManagedService());
        this.clusterService = clusterService;
        this.address = address;
        this.channel = channel;
    }

    private ManagedChannel initializeManagedChannel(String address, int port) {
        return ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
    }

    protected <T extends GeneratedMessageV3> T withPortunusServiceStub(Function<PortunusServiceBlockingStub, T> executable) {
        if (!clusterService.getPortunusClusterInstance().isShutdown()) {
            try {
                PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
                return executable.apply(portunusServiceStub);
            } catch (Exception e) {
                if (e instanceof OperationFailedException) {
                    throw e;
                }
                throw new OperationFailedException(String.format("Operation execution on remote server '%s' failed", address), e);
            }
        } else {
            throw new OperationFailedException("Server is shutting down. Operation will be cancelled");
        }
    }

    protected void withPortunusServiceStub(Consumer<PortunusServiceBlockingStub> executable) {
        if (!clusterService.getPortunusClusterInstance().isShutdown()) {
            PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
            executable.accept(portunusServiceStub);
        } else {
            throw new OperationFailedException("Server is shutting down. Operation will be cancelled");
        }
    }

    private PortunusServiceBlockingStub newPortunusServiceStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    protected AddressDTO getLocalServerAddressDTO() {
        LocalPortunusServer localServer = clusterService.getLocalServer();
        return clusterService.getConversionService().convert(localServer.getAddress());
    }

    protected <T extends Serializable> List<ByteString> toDistributed(Collection<T> elements) {
        return elements.stream()
                .map(it -> DistributedWrapper.from(it).getByteString())
                .toList();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(this::shutdownManagedChannel);
    }

    protected void shutdownManagedChannel(ManagedChannel managedChannel) {
        if (!managedChannel.isShutdown()) {
            awaitTermination(managedChannel);
        }
        if (!managedChannel.isTerminated()) {
            forcefulShutdown(managedChannel);
        }
    }

    private void awaitTermination(ManagedChannel managedChannel) {
        try {
            managedChannel.shutdown();

            if (!managedChannel.awaitTermination(3, TimeUnit.SECONDS)) {
                getLogger().warn("Timed out gracefully shutting down connection: {}. ", managedChannel);
            }
        } catch (Exception e) {
            getLogger().error("Unexpected exception while waiting for channel termination", e);
        }
    }

    private void forcefulShutdown(ManagedChannel managedChannel) {
        try {
            getLogger().warn("Timed out forcefully shutting down connection: {}. ", managedChannel);
            managedChannel.shutdownNow();
        } catch (Exception e) {
            getLogger().error("Unexpected exception while waiting for channel termination", e);
        }
    }
}
