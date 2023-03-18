package org.slusarczykr.portunus.cache.cluster.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Collection;
import java.util.Optional;

public class PortunusGRPCClient implements PortunusClient, Managed {

    private final ManagedChannel channel;

    public PortunusGRPCClient(Address address) {
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    public PortunusGRPCClient(ManagedChannel channel) {
        this.channel = channel;
    }

    private ManagedChannel initializeManagedChannel(String address, int port) {
        return ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
    }

    @Override
    public <K, V> Cache<K, V> getCache() {
        return null;
    }

    @Override
    public Collection<Partition> getPartitions() {
        PortunusServiceBlockingStub portunusService = PortunusServiceGrpc.newBlockingStub(channel);
        return portunusService.getPartitions(createGetPartitionsCommand()).getPartitionsList();
    }

    private static GetPartitionsCommand createGetPartitionsCommand() {
        return GetPartitionsCommand.newBuilder()
                .setAddress("test")
                .build();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
