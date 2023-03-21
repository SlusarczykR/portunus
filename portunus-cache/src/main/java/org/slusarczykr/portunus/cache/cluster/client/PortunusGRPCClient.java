package org.slusarczykr.portunus.cache.cluster.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.io.Serializable;
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
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        PortunusServiceBlockingStub portunusService = newClientStub();
        ContainsEntryQuery query = createContainsEntryQuery(cacheName, key);

        ContainsEntryDocument containsEntryDocument = portunusService.containsEntry(query);

        return containsEntryDocument.getContainsEntry();
    }

    private <K extends Serializable> ContainsEntryQuery createContainsEntryQuery(String cacheName, K key) {
        Distributed<K> distributed = new Distributed.DistributedWrapper<>(key);
        ByteString keyPayload = ByteString.copyFrom(distributed.getBytes());

        return ContainsEntryQuery.newBuilder()
                .setCacheName(cacheName)
                .setEntryKeyType(key.getClass().getCanonicalName())
                .setEntryKey(keyPayload)
                .build();
    }

    @Override
    public <K, V> Cache<K, V> getCache() {
        PortunusServiceBlockingStub portunusService = newClientStub();
        return null;
    }

    @Override
    public Collection<Partition> getPartitions() {
        PortunusServiceBlockingStub portunusService = newClientStub();
        return portunusService.getPartitions(createGetPartitionsCommand()).getPartitionsList();
    }

    private PortunusServiceBlockingStub newClientStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
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
