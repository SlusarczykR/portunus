package org.slusarczykr.portunus.cache.cluster.client;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheChunkDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.*;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryQuery;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class PortunusGRPCClient extends AbstractManaged implements PortunusClient {

    private final ManagedChannel channel;

    public PortunusGRPCClient(Address address) {
        super();
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    public PortunusGRPCClient(ManagedChannel channel) {
        super();
        this.channel = channel;
    }

    private ManagedChannel initializeManagedChannel(String address, int port) {
        return ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
    }

    @Override
    public boolean anyEntry(String cacheName) {
        return withPortunusServiceStub(portunusService -> {
            ContainsAnyEntryQuery query = createContainsAnyEntryQuery(cacheName);
            return portunusService.anyEntry(query);
        }).getAnyEntry();
    }

    private ContainsAnyEntryQuery createContainsAnyEntryQuery(String cacheName) {
        return ContainsAnyEntryQuery.newBuilder()
                .setCacheName(cacheName)
                .build();
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        return withPortunusServiceStub(portunusService -> {
            ContainsEntryQuery query = createContainsEntryQuery(cacheName, key);
            return portunusService.containsEntry(query);
        }).getContainsEntry();
    }

    private <K extends Serializable> ContainsEntryQuery createContainsEntryQuery(String cacheName, K key) {
        Distributed<K> distributed = DistributedWrapper.from(key);

        return ContainsEntryQuery.newBuilder()
                .setCacheName(cacheName)
                .setEntryKeyType(key.getClass().getCanonicalName())
                .setEntryKey(distributed.getByteString())
                .build();
    }

    @Override
    public Set<CacheEntryDTO> getCache(String name) {
        return new HashSet<>(withPortunusServiceStub(portunusService -> {
            GetCacheCommand command = createGetCacheCommand(name);
            return portunusService.getCache(command);
        }).getCacheEntriesList());
    }

    @Override
    public CacheChunkDTO getCacheChunk(int partitionId) {
        return withPortunusServiceStub(portunusService -> {
            GetCacheEntriesByPartitionIdCommand command = createGetEntryCommand(partitionId);
            return portunusService.getCacheEntries(command);
        }).getCacheChunk();
    }

    private GetCacheEntriesByPartitionIdCommand createGetEntryCommand(int partitionId) {
        return GetCacheEntriesByPartitionIdCommand.newBuilder()
                .setPartitionId(partitionId)
                .build();
    }

    @Override
    public <K extends Serializable> CacheEntryDTO getCacheEntry(String name, K key) {
        return withPortunusServiceStub(portunusService -> {
            GetEntryCommand command = createGetEntryCommand(name, key);
            return portunusService.getCacheEntry(command);
        }).getCacheEntry();
    }

    private <K extends Serializable> GetEntryCommand createGetEntryCommand(String cacheName, K key) {
        Distributed<K> distributed = Distributed.DistributedWrapper.from(key);

        return GetEntryCommand.newBuilder()
                .setCacheName(cacheName)
                .setKey(distributed.getByteString())
                .build();
    }

    private static GetCacheCommand createGetCacheCommand(String name) {
        return GetCacheCommand.newBuilder()
                .setName(name)
                .build();
    }

    @Override
    public Collection<PartitionDTO> getPartitions() {
        return withPortunusServiceStub(portunusService -> {
            GetPartitionsCommand command = createGetPartitionsCommand();
            return portunusService.getPartitions(command);
        }).getPartitionsList();
    }

    private static GetPartitionsCommand createGetPartitionsCommand() {
        return GetPartitionsCommand.newBuilder()
                .build();
    }

    @Override
    public void sendEvent(ClusterEvent event) {
        withPortunusServiceStub(portunusService -> {
            portunusService.sendClusterEvent(event);
        });
    }

    @Override
    public void sendEvent(PartitionEvent event) {
        withPortunusServiceStub(portunusService -> {
            portunusService.sendPartitionEvent(event);
        });
    }

    @Override
    public boolean putEntry(String cacheName, CacheEntryDTO entry) {
        return withPortunusServiceStub(portunusService -> {
            PutEntryCommand command = createPutEntryCommand(cacheName, entry);
            return portunusService.putEntry(command);
        }).getStatus();
    }

    private PutEntryCommand createPutEntryCommand(String cacheName, CacheEntryDTO entry) {
        return PutEntryCommand.newBuilder()
                .setCacheName(cacheName)
                .setCacheEntry(entry)
                .build();
    }

    @Override
    public boolean putEntries(String cacheName, Collection<CacheEntryDTO> entry) {
        return withPortunusServiceStub(portunusService -> {
            PutEntriesCommand command = createPutEntriesCommand(cacheName, entry);
            return portunusService.putEntries(command);
        }).getStatus();
    }

    private PutEntriesCommand createPutEntriesCommand(String cacheName, Collection<CacheEntryDTO> entries) {
        return PutEntriesCommand.newBuilder()
                .setCacheName(cacheName)
                .addAllCacheEntries(entries)
                .build();
    }

    @Override
    public <K extends Serializable> CacheEntryDTO removeEntry(String cacheName, K key) {
        return withPortunusServiceStub(portunusService -> {
            RemoveEntryCommand command = createRemoveEntryCommand(cacheName, key);
            return portunusService.removeEntry(command);
        }).getCacheEntry();
    }

    @Override
    public boolean replicate(PartitionDTO partition) {
        return withPortunusServiceStub(portunusService -> {
            ReplicatePartitionCommand command = createReplicatePartitionCommand(partition);
            return portunusService.replicate(command);
        }).getStatus();
    }

    private ReplicatePartitionCommand createReplicatePartitionCommand(PartitionDTO partition) {
        return ReplicatePartitionCommand.newBuilder()
                .setPartition(partition)
                .build();
    }

    private <K extends Serializable> RemoveEntryCommand createRemoveEntryCommand(String cacheName, K key) {
        Distributed<K> distributed = DistributedWrapper.from(key);

        return RemoveEntryCommand.newBuilder()
                .setCacheName(cacheName)
                .setKey(distributed.getByteString())
                .build();
    }

    private <T extends GeneratedMessageV3> T withPortunusServiceStub(Function<PortunusServiceBlockingStub, T> executable) {
        PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
        return executable.apply(portunusServiceStub);
    }

    private void withPortunusServiceStub(Consumer<PortunusServiceBlockingStub> executable) {
        PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
        executable.accept(portunusServiceStub);
    }

    private PortunusServiceBlockingStub newPortunusServiceStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
