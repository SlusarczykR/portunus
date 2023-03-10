package org.slusarczykr.portunus.cache.cluster.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Optional;

public class PortunusGRPCClient implements PortunusClient, Managed {

    private final ManagedChannel channel;

    public PortunusGRPCClient(String address, int port) {
        this.channel = ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
    }

    @Override
    public Partition getPartitions() {
        return null;
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
