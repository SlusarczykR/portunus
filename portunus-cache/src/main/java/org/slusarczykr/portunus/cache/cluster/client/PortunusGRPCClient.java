package org.slusarczykr.portunus.cache.cluster.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PortunusGRPCClient extends AbstractManaged implements PortunusClient {

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

        ContainsEntryDocument document = portunusService.containsEntry(query);

        return document.getContainsEntry();
    }

    private <K extends Serializable> ContainsEntryQuery createContainsEntryQuery(String cacheName, K key) {
        Distributed<K> distributed = Distributed.DistributedWrapper.from(key);

        return ContainsEntryQuery.newBuilder()
                .setCacheName(cacheName)
                .setEntryKeyType(key.getClass().getCanonicalName())
                .setEntryKey(distributed.getByteString())
                .build();
    }

    @Override
    public Set<CacheEntryDTO> getCache(String name) {
        PortunusServiceBlockingStub portunusService = newClientStub();
        GetCacheCommand command = createGetCacheCommand(name);

        GetCacheDocument document = portunusService.getCache(command);

        return new HashSet<>(document.getCacheEntriesList());
    }

    private static GetCacheCommand createGetCacheCommand(String name) {
        return GetCacheCommand.newBuilder()
                .setName(name)
                .build();
    }

    @Override
    public Collection<PartitionDTO> getPartitions() {
        PortunusServiceBlockingStub portunusService = newClientStub();
        GetPartitionsCommand command = createGetPartitionsCommand();

        GetPartitionsDocument document = portunusService.getPartitions(command);

        return document.getPartitionsList();
    }

    @Override
    public void sendEvent(ClusterEvent event) {
        PortunusServiceBlockingStub portunusService = newClientStub();

        portunusService.sendEvent(event);
    }

    private PortunusServiceBlockingStub newClientStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    private static GetPartitionsCommand createGetPartitionsCommand() {
        return GetPartitionsCommand.newBuilder()
                .build();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
