package org.slusarczykr.portunus.cache.cluster.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheChunkDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.*;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.*;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class PortunusGRPCClient extends AbstractManaged implements PortunusClient {

    private final ClusterService clusterService;
    private final ManagedChannel channel;

    public PortunusGRPCClient(ClusterService clusterService, Address address) {
        super(clusterService.getManagedService());
        this.clusterService = clusterService;
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    public PortunusGRPCClient(ClusterService clusterService, ManagedChannel channel) {
        super(clusterService.getManagedService());
        this.clusterService = clusterService;
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
                .setFrom(getLocalServerAddressDTO())
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
                .setFrom(getLocalServerAddressDTO())
                .setCacheName(cacheName)
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
    public <K extends Serializable> Set<CacheEntryDTO> getCacheEntries(String name, Collection<K> keys) {
        return new HashSet<>(withPortunusServiceStub(portunusService -> {
            GetEntriesQuery query = createGetEntriesQuery(name, keys);
            return portunusService.getCacheEntries(query);
        }).getCacheEntryList());
    }

    private <K extends Serializable> GetEntriesQuery createGetEntriesQuery(String name, Collection<K> keys) {
        return GetEntriesQuery.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setCacheName(name)
                .addAllKey(toDistributed(keys))
                .build();
    }

    @Override
    public CacheChunkDTO getCacheChunk(int partitionId) {
        return withPortunusServiceStub(portunusService -> {
            GetCacheEntriesByPartitionIdCommand command = createGetEntryCommand(partitionId);
            return portunusService.getCacheEntriesByPartitionId(command);
        }).getCacheChunk();
    }

    private GetCacheEntriesByPartitionIdCommand createGetEntryCommand(int partitionId) {
        return GetCacheEntriesByPartitionIdCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
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
                .setFrom(getLocalServerAddressDTO())
                .setCacheName(cacheName)
                .setKey(distributed.getByteString())
                .build();
    }

    private GetCacheCommand createGetCacheCommand(String name) {
        return GetCacheCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
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

    private GetPartitionsCommand createGetPartitionsCommand() {
        return GetPartitionsCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
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
                .setFrom(getLocalServerAddressDTO())
                .setCacheName(cacheName)
                .setCacheEntry(entry)
                .build();
    }

    @Override
    public boolean putEntries(String cacheName, Collection<CacheEntryDTO> entries) {
        return withPortunusServiceStub(portunusService -> {
            PutEntriesCommand command = createPutEntriesCommand(cacheName, entries);
            return portunusService.putEntries(command);
        }).getStatus();
    }

    private PutEntriesCommand createPutEntriesCommand(String cacheName, Collection<CacheEntryDTO> entries) {
        return PutEntriesCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
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
    public boolean replicate(CacheChunkDTO cacheChunk) {
        return withPortunusServiceStub(portunusService -> {
            ReplicatePartitionCommand command = createReplicatePartitionCommand(cacheChunk);
            return portunusService.replicate(command);
        }).getStatus();
    }

    private ReplicatePartitionCommand createReplicatePartitionCommand(CacheChunkDTO cacheChunk) {
        return ReplicatePartitionCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setCacheChunk(cacheChunk)
                .build();
    }

    @Override
    public boolean migrate(List<CacheChunkDTO> cacheChunks, boolean replicate) {
        return withPortunusServiceStub(portunusService -> {
            MigratePartitionsCommand command = createMigratePartitionsCommand(cacheChunks, replicate);
            return portunusService.migrate(command);
        }).getStatus();
    }

    private MigratePartitionsCommand createMigratePartitionsCommand(List<CacheChunkDTO> cacheChunks, boolean replicate) {
        return MigratePartitionsCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setReplicate(replicate)
                .addAllCacheChunks(cacheChunks)
                .build();
    }

    @Override
    public void register() {
        withPortunusServiceStub(portunusService -> {
            return portunusService.register(createRegisterMemberCommand());
        });
    }

    @Override
    public Collection<CacheEntryDTO> removeEntries(String cacheName, Collection<CacheEntryDTO> entries) {
        return withPortunusServiceStub(portunusService -> {
            RemoveEntriesCommand command = createRemoveEntriesCommand(cacheName, entries);
            return portunusService.removeEntries(command);
        }).getCacheEntryList();
    }

    private RemoveEntriesCommand createRemoveEntriesCommand(String cacheName, Collection<CacheEntryDTO> entries) {
        return RemoveEntriesCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setCacheName(cacheName)
                .addAllKey(getEntryKeys(entries))
                .build();
    }

    private List<ByteString> getEntryKeys(Collection<CacheEntryDTO> entries) {
        return entries.stream()
                .map(CacheEntryDTO::getKey)
                .toList();
    }

    private RegisterMemberCommand createRegisterMemberCommand() {
        return RegisterMemberCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .build();
    }

    private <K extends Serializable> RemoveEntryCommand createRemoveEntryCommand(String cacheName, K key) {
        Distributed<K> distributed = DistributedWrapper.from(key);

        return RemoveEntryCommand.newBuilder()
                .setFrom(getLocalServerAddressDTO())
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

    private AddressDTO getLocalServerAddressDTO() {
        LocalPortunusServer localServer = clusterService.getLocalServer();
        return clusterService.getConversionService().convert(localServer.getAddress());
    }

    private <T extends Serializable> List<ByteString> toDistributed(Collection<T> elements) {
        return elements.stream()
                .map(it -> DistributedWrapper.from(it).getByteString())
                .toList();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
