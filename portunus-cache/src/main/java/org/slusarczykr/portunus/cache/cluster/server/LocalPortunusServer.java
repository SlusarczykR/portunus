package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.DistributedCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionChangeDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionCreatedEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionUpdatedEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.manager.DefaultDistributedCacheManager;
import org.slusarczykr.portunus.cache.manager.DistributedCacheManager;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent.PartitionEventType.PartitionCreated;
import static org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent.PartitionEventType.PartitionUpdated;

public class LocalPortunusServer extends AbstractPortunusServer {

    private static final Logger log = LoggerFactory.getLogger(LocalPortunusServer.class);

    private DistributedCacheManager cacheManager;

    private final PaxosServer paxosServer;

    private Server gRPCServer;

    private LocalPortunusServer(ClusterService clusterService, ClusterMemberContext context) {
        super(clusterService, context);
        this.paxosServer = new PaxosServer(context.getPort());
    }

    public static LocalPortunusServer newInstance(ClusterService clusterService, ClusterConfig clusterConfig) {
        ClusterMemberContext serverContext = createServerContext(clusterService, clusterConfig);
        return new LocalPortunusServer(clusterService, serverContext);
    }

    @SneakyThrows
    private static ClusterMemberContext createServerContext(ClusterService clusterService, ClusterConfig clusterConfig) {
        clusterConfig = getDefaultClusterConfigIfAbsent(clusterService, clusterConfig);
        Address address = clusterConfig.getLocalServerAddress();

        return new ClusterMemberContext(address);
    }

    private static ClusterConfig getDefaultClusterConfigIfAbsent(ClusterService clusterService, ClusterConfig clusterConfig) {
        return Optional.ofNullable(clusterConfig).orElseGet(() -> getClusterConfig(clusterService));
    }

    private static ClusterConfig getClusterConfig(ClusterService clusterService) {
        return clusterService.getClusterConfigService().getClusterConfig();
    }

    @Override
    protected void initialize() throws PortunusException {
        try {
            log.info("Starting gRPC server for '{}' server", serverContext.getPlainAddress());
            this.cacheManager = new DefaultDistributedCacheManager();
            this.gRPCServer = createGRPCServer();
            this.gRPCServer.start();
        } catch (IOException e) {
            throw new PortunusException("Could not start gRPC server", e);
        }
    }

    private Server createGRPCServer() {
        return ServerBuilder.forPort(serverContext.getPort())
                .addService(new PortunusGRPCService(clusterService))
                .build();
    }

    public PaxosServer getPaxosServer() {
        return paxosServer;
    }

    public void updatePaxosServerId(int numberOfServers) {
        getPaxosServer().updateServerId(getAddress().port(), numberOfServers);
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(gRPCServer).ifPresent(Server::shutdown);
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public boolean anyEntry(String cacheName) {
        return !cacheManager.getCache(cacheName).isEmpty();
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        return cacheManager.getCache(cacheName).containsKey(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name) {
        return cacheManager.getCache(name);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> getCacheEntry(String name, K key) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return cache.getEntry(key).orElse(null);
    }

    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name, boolean includeReplica) {
        // TODO filter replica entries
        Cache<K, V> cache = cacheManager.getCache(name);
        return new HashSet<>(cache.allEntries());
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        return getCacheEntries(name, true);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name, Collection<K> keys) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return new HashSet<>(cache.getEntries(keys));
    }

    @Override
    public <K extends Serializable, V extends Serializable> void put(String name, Partition partition, Cache.Entry<K, V> entry) {
        put(name, entry);
        registerCacheEntries(name, partition, Set.of(entry));
    }

    private <K extends Serializable, V extends Serializable> void registerCacheEntries(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        boolean partitionExists = cacheManager.anyCacheEntry(partition.getPartitionId());
        cacheManager.register(partition.getPartitionId(), name, entries);

        if (partition.isLocal()) {
            sendPartitionEvent(partition.new Change<>(name, entries, false), !partitionExists);
        }
    }

    private <K extends Serializable, V extends Serializable> void sendPartitionEvent(Partition.Change<K, V> partitionChange,
                                                                                     boolean newPartition) {
        log.debug("Sending partition updated event for cache: '{}' [{} altered entries]",
                partitionChange.getCacheName(), partitionChange.getEntries().size());
        if (newPartition) {
            sendPartitionEvent(partitionChange, this::createPartitionCreatedEvent);
        } else {
            sendPartitionEvent(partitionChange, this::createPartitionUpdatedEvent);
        }
    }

    private <K extends Serializable, V extends Serializable> void put(String name, Cache.Entry<K, V> entry) {
        Cache<K, V> cache = cacheManager.getCache(name);
        log.debug("Putting entry from local cache: '{}'", cache.getName());
        cache.put(entry);
    }

    @Override
    public <K extends Serializable, V extends Serializable> void putAll(String name, Partition partition, Map<K, V> entries) {
        Cache<K, V> cache = cacheManager.getCache(name);
        log.debug("Putting {} entries to local cache: '{}'", entries.size(), cache.getName());
        cache.putAll(entries);
        Set<Cache.Entry<K, V>> cacheEntries = toEntrySet(entries);
        registerCacheEntries(name, partition, cacheEntries);
    }

    private static <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> toEntrySet(Map<K, V> entries) {
        return entries.entrySet().stream()
                .map(it -> new DefaultCache.Entry<>(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, Partition partition, K key) {
        Cache<K, V> cache = cacheManager.getCache(name);
        log.debug("Removing entry from local cache: '{}'", cache.getName());
        return Optional.ofNullable(cache.remove(key))
                .map(it -> {
                    unregisterCacheEntries(name, partition, Set.of(it));
                    return it;
                })
                .orElse(null);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> removeAll(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        Cache<K, V> cache = cacheManager.getCache(name);
        Set<K> keys = DistributedCache.getEntryKeys(entries);
        log.debug("Removing {} entries from local cache: '{}'", keys.size(), cache.getName());
        Collection<Cache.Entry<K, V>> removedEntries = cache.removeAll(keys);
        unregisterCacheEntries(name, partition, new HashSet<>(removedEntries));

        return new HashSet<>(removedEntries);
    }

    private <K extends Serializable, V extends Serializable> void unregisterCacheEntries(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        cacheManager.unregister(partition.getPartitionId(), name, DistributedCache.getEntryKeys(entries));

        if (partition.isLocal()) {
            sendPartitionEvent(partition.new Change<>(name, entries, true), false);
        }
    }

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        return cacheManager.getCacheEntries(partitionId);
    }

    @Override
    public void replicate(CacheChunk cacheChunk) {
        clusterService.getReplicaService().registerPartitionReplica(cacheChunk.partition());
        cacheChunk.partition().addReplicaOwner(getAddress());
    }

    public <K extends Serializable, V extends Serializable> void update(Partition.Change<K, V> partitionChange) {
        Cache<K, V> cache = getCache(partitionChange.getCacheName());

        if (partitionChange.isRemove()) {
            Set<K> keys = DistributedCache.getEntryKeys(partitionChange.getEntries());
            removeFromLocalCache(cache.getName(), partitionChange.getPartition(), cache.getEntries(keys));
        } else {
            updateLocalCache(cache.getName(), partitionChange.getPartition(), partitionChange.getEntries());
        }
    }

    public void update(CacheChunk cacheChunk) {
        cacheChunk.cacheEntries().forEach(it -> updateLocalCache(it, cacheChunk.partition()));
    }

    private <K extends Serializable, V extends Serializable> void updateLocalCache(Cache<K, V> cache, Partition partition) {
        Cache<K, V> localCache = getCache(cache.getName());
        log.debug("Updating cache entries: '{}'. Cache size: {}", cache.getName(), localCache.allEntries().size());
        updateLocalCache(cache.getName(), partition, cache.allEntries());
        log.debug("Successfully updated cache: '{}'. Cache size: {}", cache.getName(), localCache.allEntries().size());
    }

    private <K extends Serializable, V extends Serializable> void removeFromLocalCache(String name, Partition partition,
                                                                                       Collection<Cache.Entry<K, V>> cacheEntries) {
        cacheEntries.forEach(it -> log.debug("Removing entry: {} from '{}'", it, name));
        removeAll(name, partition, new HashSet<>(cacheEntries));
    }

    private PartitionEvent createPartitionUpdatedEvent(PartitionChangeDTO partitionChangeDTO) {
        return PartitionEvent.newBuilder()
                .setFrom(getAddressDTO())
                .setEventType(PartitionUpdated)
                .setPartitionId(partitionChangeDTO.getPartition().getKey())
                .setPartitionUpdatedEvent(
                        PartitionUpdatedEvent.newBuilder()
                                .setPartitionChange(partitionChangeDTO)
                                .build()
                )
                .build();
    }

    private PartitionEvent createPartitionCreatedEvent(PartitionChangeDTO partitionChangeDTO) {
        return PartitionEvent.newBuilder()
                .setFrom(getAddressDTO())
                .setEventType(PartitionCreated)
                .setPartitionId(partitionChangeDTO.getPartition().getKey())
                .setPartitionCreatedEvent(
                        PartitionCreatedEvent.newBuilder()
                                .setPartitionChange(partitionChangeDTO)
                                .build()
                )
                .build();
    }

    private <K extends Serializable, V extends Serializable> void sendPartitionEvent(Partition.Change<K, V> partitionChange,
                                                                                     Function<PartitionChangeDTO, PartitionEvent> operation) {
        PartitionChangeDTO partitionChangeDTO = clusterService.getConversionService().convert(partitionChange);
        PartitionEvent partitionEvent = operation.apply(partitionChangeDTO);

        clusterService.getClusterEventPublisher().publishEvent(partitionEvent);
    }

    public CacheChunk getCacheChunk(Partition partition) {
        Set<Cache<? extends Serializable, ? extends Serializable>> cacheEntries = getCacheEntries(partition.getPartitionId());
        return new CacheChunk(partition, cacheEntries);
    }

    public void remove(Partition partition) {
        cacheManager.remove(partition.getPartitionId());
    }

    private <K extends Serializable, V extends Serializable> void updateLocalCache(String name, Partition partition,
                                                                                   Collection<Cache.Entry<K, V>> cacheEntries) {
        Map<K, V> cacheEntriesMap = cacheEntries.stream()
                .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));
        cacheEntriesMap.entrySet().forEach(it -> log.debug("Putting entry: {} to '{}'", it, name));
        putAll(name, partition, cacheEntriesMap);
    }

    private AddressDTO getAddressDTO() {
        return clusterService.getConversionService().convert(getAddress());
    }

    @Override
    public String toString() {
        return "LocalPortunusServer{" +
                "address=" + serverContext.getPlainAddress() +
                '}';
    }
}
