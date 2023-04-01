package org.slusarczykr.portunus.cache.cluster.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.PutEntryCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.PutEntryDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.RemoveEntryCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.RemoveEntryDocument;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryQuery;
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
    public boolean anyEntry(String cacheName) {
        PortunusServiceBlockingStub portunusService = newClientStub();
        ContainsAnyEntryQuery query = createContainsAnyEntryQuery(cacheName);

        ContainsAnyEntryDocument document = portunusService.anyEntry(query);

        return document.getAnyEntry();
    }

    private ContainsAnyEntryQuery createContainsAnyEntryQuery(String cacheName) {
        return ContainsAnyEntryQuery.newBuilder()
                .setCacheName(cacheName)
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

    @Override
    public boolean putEntry(String cacheName, CacheEntryDTO entry) {
        PortunusServiceBlockingStub portunusService = newClientStub();
        PutEntryCommand command = createPutEntryCommand(cacheName, entry);

        PutEntryDocument document = portunusService.putEntry(command);

        return document.getStatus();
    }

    private PutEntryCommand createPutEntryCommand(String cacheName, CacheEntryDTO entry) {
        return PutEntryCommand.newBuilder()
                .setCacheName(cacheName)
                .setCacheEntry(entry)
                .build();
    }

    private PortunusServiceBlockingStub newClientStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    private static GetPartitionsCommand createGetPartitionsCommand() {
        return GetPartitionsCommand.newBuilder()
                .build();
    }

    @Override
    public <K extends Serializable> CacheEntryDTO removeEntry(String cacheName, K key) {
        PortunusServiceBlockingStub portunusService = newClientStub();
        RemoveEntryCommand command = createRemoveEntryCommand(cacheName, key);

        RemoveEntryDocument document = portunusService.removeEntry(command);

        return document.getCacheEntry();
    }

    private <K extends Serializable> RemoveEntryCommand createRemoveEntryCommand(String cacheName, K key) {
        Distributed<K> distributed = Distributed.DistributedWrapper.from(key);

        return RemoveEntryCommand.newBuilder()
                .setCacheName(cacheName)
                .setKey(distributed.getByteString())
                .build();
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
