package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheChunkDTO;
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

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return new HashSet<>(cache.allEntries());
    }

    @Override
    public <K extends Serializable, V extends Serializable> void put(String name, Partition partition, Cache.Entry<K, V> entry) {
        put(name, entry);
        registerCacheEntry(name, partition, Set.of(entry));
    }

    private <K extends Serializable, V extends Serializable> void registerCacheEntry(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        boolean partitionExists = cacheManager.anyCacheEntry(partition.getPartitionId());
        register(name, partition, entries);

        if (partition.isLocal()) {
            sendPartitionEvent(partition, !partitionExists);
        }
    }

    private void sendPartitionEvent(Partition partition, boolean newPartition) {
        if (newPartition) {
            sendPartitionEvent(partition, this::createPartitionCreatedEvent);
        } else {
            sendPartitionEvent(partition, this::createPartitionUpdatedEvent);
        }
    }

    private <K extends Serializable, V extends Serializable> void put(String name, Cache.Entry<K, V> entry) {
        Cache<K, V> cache = cacheManager.getCache(name);
        cache.put(entry);
    }

    private <K extends Serializable, V extends Serializable> void register(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        cacheManager.register(partition.getPartitionId(), name, entries);
    }

    @Override
    public <K extends Serializable, V extends Serializable> void putAll(String name, Partition partition, Map<K, V> entries) {
        Cache<K, V> cache = cacheManager.getCache(name);
        log.debug("Putting {} entries to local cache", entries.size());
        cache.putAll(entries);
        Set<Cache.Entry<K, V>> cacheEntries = toEntrySet(entries);
        registerCacheEntry(name, partition, cacheEntries);
    }

    private static <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> toEntrySet(Map<K, V> entries) {
        return entries.entrySet().stream()
                .map(it -> new DefaultCache.Entry<>(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key) {
        Cache<K, V> cache = cacheManager.getCache(name);
        //TODO unregister cache entries from partition and send partition event
        return cache.remove(key);
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

    public <K extends Serializable, V extends Serializable> void update(CacheChunk cacheChunk) {
        cacheChunk.cacheEntries().forEach(it -> {
            Cache<K, V> localCache = getCache(it.getName());
            log.debug("Updating cache entries: '{}'. Cache size: {}", it.getName(), localCache.allEntries().size());
            updateLocalCache(it.getName(), cacheChunk.partition(), it.allEntries());
            log.debug("Successfully updated cache: '{}'. Cache size: {}", it.getName(), localCache.allEntries().size());
        });
    }

    private PartitionEvent createPartitionUpdatedEvent(CacheChunkDTO cacheChunkDTO) {
        return PartitionEvent.newBuilder()
                .setFrom(getAddressDTO())
                .setEventType(PartitionUpdated)
                .setPartitionId(cacheChunkDTO.getPartition().getKey())
                .setPartitionUpdatedEvent(
                        PartitionUpdatedEvent.newBuilder()
                                .setCacheChunk(cacheChunkDTO)
                                .build()
                )
                .build();
    }

    private PartitionEvent createPartitionCreatedEvent(CacheChunkDTO cacheChunkDTO) {
        return PartitionEvent.newBuilder()
                .setFrom(getAddressDTO())
                .setEventType(PartitionCreated)
                .setPartitionId(cacheChunkDTO.getPartition().getKey())
                .setPartitionCreatedEvent(
                        PartitionCreatedEvent.newBuilder()
                                .setCacheChunk(cacheChunkDTO)
                                .build()
                )
                .build();
    }

    private void sendPartitionEvent(Partition partition, Function<CacheChunkDTO, PartitionEvent> operation) {
        CacheChunk cacheChunk = getCacheChunk(partition);
        CacheChunkDTO cacheChunkDTO = clusterService.getConversionService().convert(cacheChunk);
        PartitionEvent partitionEvent = operation.apply(cacheChunkDTO);

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
