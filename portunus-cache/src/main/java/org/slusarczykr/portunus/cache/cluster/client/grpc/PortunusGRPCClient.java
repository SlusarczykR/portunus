package org.slusarczykr.portunus.cache.cluster.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Collection;
import java.util.Optional;

public class PortunusGRPCClient implements PortunusClient, Managed {

    private final ManagedChannel channel;

    public PortunusGRPCClient(String address, int port) {
        this.channel = initializeManagedChannel(address, port);
    }

    private ManagedChannel initializeManagedChannel(String address, int port) {
        return ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
    }

    @Override
    public Collection<Partition> getPartitions() {
        PortunusServiceBlockingStub portunusService = PortunusServiceGrpc.newBlockingStub(channel);
        return portunusService.getPartitions(GetPartitionsCommand.newBuilder().build()).getPartitionsList();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
