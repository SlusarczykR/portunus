package org.slusarczykr.portunus.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistributedCache<K extends Serializable, V extends Serializable> extends AbstractManaged implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DistributedCache.class);

    private final ClusterService clusterService;

    private final String name;
    private final ExecutorService operationExecutor;
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DistributedCache(ClusterService clusterService, String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        super(clusterService.getManagedService());
        this.clusterService = clusterService;
        this.name = name;
        this.operationExecutor = newOperationExecutor();
        eventListeners.forEach(cacheEntryObserver::register);
    }

    private static ExecutorService newOperationExecutor() {
        return Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("distributed-cache-%d")
                        .build()
        );
    }

    public boolean anyLocalEntry() {
        return executeOperation(OperationType.IS_EMPTY, () -> {
            boolean anyLocalEntry = clusterService.getDiscoveryService().localServer().anyEntry(name);
            return anyLocalEntry;
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return executeOperation(OperationType.IS_EMPTY, () -> {
            boolean anyLocalEntry = clusterService.getDiscoveryService().localServer().anyEntry(name);
            return !anyLocalEntry || remoteServersStream().anyMatch(it -> it.anyEntry(name));
        });
    }

    @Override
    public boolean containsKey(K key) {
        return executeOperation(OperationType.CONTAINS_KEY, () ->
                executeLocalOrDistributed(key, true, it -> it.containsEntry(name, key))
        );
    }

    @Override
    public boolean containsValue(V value) {
        return executeOperation(OperationType.CONTAINS_VALUE, () ->
                clusterService.getDiscoveryService().localServer().getCacheEntries(name).stream()
                        .anyMatch(it -> it.getValue().equals(value))
        );
    }

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return executeOperation(OperationType.GET_ENTRY, () ->
                executeLocalOrDistributed(key, true, it -> Optional.ofNullable(it.getCacheEntry(name, key)))
        );
    }

    @Override
    public Collection<Cache.Entry<K, V>> getEntries(Collection<K> keys) {
        return executeOperation(OperationType.GET_ENTRIES, () -> {
            Set<Cache.Entry<K, V>> remoteEntries = new HashSet<>(withRemoteServers(it -> getRemoteEntries(it, keys)));
            Set<Cache.Entry<K, V>> entries = clusterService.getLocalServer().getCacheEntries(name, keys);
            entries.forEach(cacheEntryObserver::onAccess);
            remoteEntries.addAll(entries);

            return Collections.unmodifiableCollection(remoteEntries);
        });
    }

    private Set<Cache.Entry<K, V>> getRemoteEntries(PortunusServer remoteServer, Collection<K> keys) {
        return remoteServer.getCacheEntries(name, keys);
    }

    @Override
    public Collection<Cache.Entry<K, V>> allEntries() {
        return executeOperation(OperationType.GET_ALL_ENTRIES, () -> {
            Set<Cache.Entry<K, V>> remoteEntries = new HashSet<>(withRemoteServers(this::getAllRemoteEntries));
            Set<Cache.Entry<K, V>> entries = clusterService.getLocalServer().getCacheEntries(name);
            entries.forEach(cacheEntryObserver::onAccess);
            remoteEntries.addAll(entries);

            return Collections.unmodifiableCollection(remoteEntries);
        });
    }

    private Set<Cache.Entry<K, V>> getAllRemoteEntries(PortunusServer remoteServer) {
        return remoteServer.getCacheEntries(name);
    }

    @Override
    public void put(K key, V value) {
        executeOperation(OperationType.PUT, () -> {
            validate(key, value);
            Entry<K, V> entry = new Entry<>(key, value);
            putEntry(key, entry);
            cacheEntryObserver.onAdd(entry);

            return entry;
        });
    }

    @Override
    public void putAll(Set<Cache.Entry<K, V>> entries) {
        Map<K, V> cacheEntries = entries.stream()
                .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));
        putAll(cacheEntries);
    }

    private void putEntry(K key, Entry<K, V> entry) {
        Partition partition = getPartitionForKey(key);
        putEntry(partition, entry);
    }

    @SneakyThrows
    private void putEntry(Partition partition, Entry<K, V> entry) {
        log.debug("Putting entry: {} on: '{}', partition: {}", entry, partition.getOwnerAddress(), partition);
        PortunusServer owner = partition.getOwner();
        owner.put(name, partition, entry);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        executeOperation(OperationType.PUT_ALL, () -> {
            validate(entries);
            entries.forEach((key, value) -> {
                Entry<K, V> entry = new Entry<>(key, value);
                putEntry(key, entry);
                cacheEntryObserver.onAdd(entry);
            });
            return entries;
        });
    }

    private void validate(Map<K, V> entries) {
        entries.forEach(this::validate);
    }

    @Override
    public Cache.Entry<K, V> remove(K key) {
        return executeOperation(OperationType.REMOVE, () -> removeEntry(key));
    }

    @SneakyThrows
    private Cache.Entry<K, V> removeEntry(K key) {
        log.debug("Removing entry: {} from local cache", key);
        return Optional.ofNullable((Cache.Entry<K, V>) executeLocalOrDistributed(key, it -> it.remove(name, getPartitionForKey(key), key)))
                .map(it -> {
                    cacheEntryObserver.onRemove(it);
                    return it;
                })
                .orElse(null);
    }

    @Override
    public Collection<Cache.Entry<K, V>> removeAll(Collection<K> keys) {
        return executeOperation(OperationType.REMOVE_ALL, () -> {
            return keys.stream()
                    .map(this::removeEntry)
                    .filter(Objects::nonNull)
                    .toList();
        });
    }

    private void validate(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    private <T> T executeOperation(OperationType operationType, Callable<T> operation) {
        try {
            log.trace("Executing operation: '{}'", operationType.name());
            Future<T> futureResult = operationExecutor.submit(operation);
            T result = futureResult.get();
            log.trace("Operation: '{}' execution has finished", operationType.name());
            return result;
        } catch (Exception e) {
            log.error("Failed to execute operation: '{}'", operationType.name(), e);
            throw new OperationFailedException(String.format("Operation: '%s' failed", operationType.name()));
        }
    }

    @SneakyThrows
    private <T> T executeLocalOrDistributed(K key, Function<PortunusServer, T> operation) {
        return executeLocalOrDistributed(key, false, operation);
    }

    @SneakyThrows
    private <T> T executeLocalOrDistributed(K key, boolean executeOnReplicaOwner, Function<PortunusServer, T> operation) {
        String objectKey = key.toString();
        Partition partition = clusterService.getPartitionService().getPartitionForKey(objectKey);
        PortunusServer owner = partition.getOwner();

        if (executeOnReplicaOwner && isPartitionReplicaOwner(partition)) {
            log.debug("Local server is replica owner of partition: {}", partition.getPartitionId());
            owner = clusterService.getPortunusClusterInstance().localMember();
        }
        log.debug("Executing operation on '{}'", owner.getAddress());

        return operation.apply(owner);
    }

    private boolean isPartitionReplicaOwner(Partition partition) {
        return clusterService.getReplicaService().isPartitionReplicaOwner(partition.getPartitionId());
    }

    private List<Cache.Entry<K, V>> withRemoteServers(Function<RemotePortunusServer, Collection<Cache.Entry<K, V>>> operation) {
        return remoteServersStream()
                .map(operation)
                .flatMap(Collection::stream)
                .toList();
    }

    private Stream<RemotePortunusServer> remoteServersStream() {
        return clusterService.getDiscoveryService().remoteServers().parallelStream();
    }

    private Partition getPartitionForKey(K key) {
        return clusterService.getPartitionService().getPartitionForKey(key);
    }

    @Override
    public void shutdown() {
        operationExecutor.shutdown();
    }

    public static <K extends Serializable, V extends Serializable> Set<K> getEntryKeys(Set<Cache.Entry<K, V>> entries) {
        return entries.stream()
                .map(Cache.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    public record Entry<K, V>(K key, V value) implements Cache.Entry<K, V> {

        public Entry(Map.Entry<K, V> entry) {
            this(entry.getKey(), entry.getValue());
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry<?, ?> entry = (Entry<?, ?>) o;
            return Objects.equals(key, entry.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    public enum OperationType {
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET_ENTRY,
        GET_ENTRIES,
        GET_ENTRIES_BY_PARTITION,
        GET_ALL_ENTRIES,
        PUT,
        PUT_ALL,
        REMOVE,
        REMOVE_ALL,
        VOTE,
        SEND_CLUSTER_EVENT,
        SEND_PARTITION_EVENT,
        SYNC_STATE,
        REPLICATE_PARTITION,
        MIGRATE_PARTITIONS,
        REGISTER_MEMBER
    }
}
