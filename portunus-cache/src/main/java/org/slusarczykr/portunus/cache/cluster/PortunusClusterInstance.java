package org.slusarczykr.portunus.cache.cluster;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DistributedCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent.ClusterEventType;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberJoinedEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.MemberLeftEvent;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.maintenance.DefaultManagedService;
import org.slusarczykr.portunus.cache.maintenance.ManagedService;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.slusarczykr.portunus.cache.cluster.PortunusClusterInstance.State.*;

public class PortunusClusterInstance implements PortunusCluster, PortunusServer {

    private static final Logger log = LoggerFactory.getLogger(PortunusClusterInstance.class);

    public static final int DEFAULT_PORT = 8091;

    private final AtomicReference<State> state = new AtomicReference<>(STARTING);

    private final ManagedService managedService;
    private final ShutdownThread shutdownThread;
    private final ClusterService clusterService;
    private final LocalPortunusServer localServer;

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    public static PortunusClusterInstance getInstance(ClusterConfig clusterConfig) {
        return new PortunusClusterInstance(clusterConfig);
    }

    private PortunusClusterInstance(ClusterConfig clusterConfig) {
        log.info("Portunus instance is starting");
        this.managedService = DefaultManagedService.newInstance();
        this.shutdownThread = new ShutdownThread();
        preInitialize();
        this.clusterService = DefaultClusterService.newInstance(this, clusterConfig);
        this.localServer = LocalPortunusServer.newInstance(clusterService);
        postInitialize();
        log.info("Portunus server started on port: {}", clusterService.getClusterConfig().getPort());
    }

    private void preInitialize() {
        log.debug("Executing pre initialize procedure");
        registerShutdownHook();
    }

    private void postInitialize() {
        try {
            log.debug("Executing post initialize procedure");
            clusterService.getDiscoveryService().register(localServer);
            clusterService.getServiceManager().injectPaxosServer(localServer.getPaxosServer());
            clusterService.getLeaderElectionStarter().start();

            publishMemberEvent(this::createMemberJoinedEvent);
            state.set(STARTED);
        } catch (Exception e) {
            throw new FatalPortunusException("Could not initialize portunus instance", e);
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public boolean isInitialized() {
        return STARTED == state.get();
    }

    public boolean isShutdown() {
        return List.of(STOPPING, STOPPED).contains(state.get());
    }

    public ManagedService getManagedService() {
        return managedService;
    }

    public LocalPortunusServer localMember() {
        return localServer;
    }

    public List<RemotePortunusServer> remoteMembers() {
        return clusterService.getDiscoveryService().remoteServers();
    }

    @Override
    public Address getAddress() {
        return localServer.getAddress();
    }

    @Override
    public String getPlainAddress() {
        return localServer.getPlainAddress();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name) {
        return (Cache<K, V>) caches.computeIfAbsent(name, this::newDistributedCache);
    }

    @Override
    public synchronized void shutdown() {
        shutdownClusterInstance();
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }

    private <K extends Serializable, V extends Serializable> DistributedCache<K, V> newDistributedCache(String name) {
        return new DistributedCache<>(clusterService, name, Collections.emptyMap());
    }

    @Override
    public boolean anyEntry(String cacheName) {
        return !getCache(cacheName).isEmpty();
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        return Optional.ofNullable(caches.get(cacheName))
                .map(it -> containsKey((Cache<K, ?>) it, key))
                .orElse(false);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> getCacheEntry(String name, K key) {
        return Optional.ofNullable(caches.get(name))
                .flatMap(it -> ((Cache<K, V>) it).getEntry(key))
                .orElse(null);
    }

    @SneakyThrows
    private static <K extends Serializable> boolean containsKey(Cache<K, ?> cache, K key) {
        return cache.containsKey(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        Cache<K, V> cache = getCache(name);
        return new HashSet<>(cache.allEntries());
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name, Collection<K> keys) {
        Cache<K, V> cache = getCache(name);
        return new HashSet<>(cache.getEntries(keys));
    }

    @Override
    public <K extends Serializable, V extends Serializable> void put(String name, Partition partition, Cache.Entry<K, V> entry) {
        Cache<K, V> cache = getCache(name);
        cache.put(entry);
    }

    @Override
    public <K extends Serializable, V extends Serializable> void putAll(String name, Partition partition, Map<K, V> entries) {
        Cache<K, V> cache = getCache(name);
        cache.putAll(entries);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, Partition partition, K key) {
        Cache<K, V> cache = getCache(name);
        return cache.remove(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> removeAll(String name, Partition partition, Set<Cache.Entry<K, V>> entries) {
        Cache<K, V> cache = getCache(name);
        return new HashSet<>(cache.removeAll(DistributedCache.getEntryKeys(entries)));
    }

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        Partition partition = clusterService.getPartitionService().getPartition(partitionId);
        PortunusServer owner = partition.getOwner();

        return owner.getCacheEntries(partitionId);
    }

    @Override
    public void replicate(CacheChunk cacheChunk) {
        clusterService.getReplicaService().replicatePartition(cacheChunk.partition());
    }

    private void publishMemberEvent(Function<AddressDTO, ClusterEvent> eventSupplier) {
        AddressDTO address = getLocalServerAddressDTO();
        ClusterEvent event = eventSupplier.apply(address);

        clusterService.getClusterEventPublisher().publishEvent(event);
    }

    private ClusterEvent createMemberJoinedEvent(AddressDTO address) {
        return ClusterEvent.newBuilder()
                .setFrom(address)
                .setEventType(ClusterEventType.MemberJoinedEvent)
                .setMemberJoinedEvent(MemberJoinedEvent.newBuilder()
                        .setAddress(address)
                        .build())
                .build();
    }

    private ClusterEvent createMemberLeftEvent(AddressDTO address) {
        return ClusterEvent.newBuilder()
                .setFrom(address)
                .setEventType(ClusterEventType.MemberLeftEvent)
                .setMemberLeftEvent(MemberLeftEvent.newBuilder()
                        .setAddress(address)
                        .build())
                .build();
    }

    private AddressDTO getLocalServerAddressDTO() {
        return clusterService.getConversionService().convert(localServer.getAddress());
    }

    private void shutdownClusterInstance() {
        state.set(STOPPING);
        log.info("Portunus cluster is shutting down");
        publishMemberEvent(PortunusClusterInstance.this::createMemberLeftEvent);
        managedService.shutdownAll();
        state.set(STOPPED);
    }

    private class ShutdownThread extends Thread {

        @Override
        public void run() {
            shutdownClusterInstance();
        }
    }

    public enum State {
        STARTING, STARTED, STOPPING, STOPPED
    }
}
