package org.slusarczykr.portunus.cache;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class DistributedCache<K extends Serializable, V extends Serializable> extends AbstractManaged implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DistributedCache.class);

    private final PartitionService partitionService;
    private final DiscoveryService discoveryService;

    private final String name;
    private final ExecutorService operationExecutor;
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DistributedCache(String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        super();
        this.name = name;
        this.operationExecutor = Executors.newSingleThreadExecutor();
        this.partitionService = DefaultPartitionService.getInstance();
        this.discoveryService = DefaultDiscoveryService.getInstance();
        eventListeners.forEach(cacheEntryObserver::register);
    }

    @Override
    public boolean isEmpty() {
        return executeOperation(OperationType.IS_EMPTY, () -> {
            boolean anyLocalEntry = discoveryService.localServer().anyEntry(name);
            boolean anyEntry = anyLocalEntry || anyRemoteEntry();

            return !anyEntry;
        });
    }

    private boolean anyRemoteEntry() {
        return remoteServersStream()
                .anyMatch(it -> it.anyEntry(name));
    }

    @Override
    public boolean containsKey(K key) {
        return executeOperation(OperationType.CONTAINS_KEY, () -> {
            String objectKey = key.toString();

            if (!partitionService.isLocalPartition(objectKey)) {
                Partition partition = partitionService.getPartitionForKey(objectKey);
                return partition.owner().containsEntry(name, key);
            }
            return discoveryService.localServer().containsEntry(name, key);
        });
    }

    @Override
    public boolean containsValue(V value) {
        return executeOperation(OperationType.CONTAINS_VALUE, () ->
                discoveryService.localServer().getCacheEntries(name).stream()
                        .anyMatch(it -> it.getValue().equals(value))
        );
    }

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return getLocalEntry(key)
                .map(it -> {
                    cacheEntryObserver.onAccess(it);
                    return it;
                });
    }

    private Optional<Cache.Entry<K, V>> getLocalEntry(K key) {
        return discoveryService.localServer().getCacheEntries(name).stream()
                .filter(it -> it.getKey().equals(key))
                .map(it -> (Cache.Entry<K, V>) it)
                .findFirst();
    }

    @Override
    public Collection<Cache.Entry<K, V>> getEntries(Collection<K> keys) {
        return executeOperation(OperationType.GET_ENTRIES, () ->
                keys.stream()
                        .map(this::getEntry)
                        .map(Optional::stream)
                        .map(it -> (Cache.Entry<K, V>) it)
                        .toList()
        );
    }

    @Override
    public Collection<Cache.Entry<K, V>> allEntries() {
        return executeOperation(OperationType.GET_ALL_ENTRIES, () -> {
            List<Cache.Entry<K, V>> remoteEntries = new ArrayList<>(getRemoteServersEntries());
            Set<Cache.Entry<K, V>> entries = discoveryService.localServer().getCacheEntries(name);
            entries.forEach(cacheEntryObserver::onAccess);
            remoteEntries.addAll(entries);

            return Collections.unmodifiableCollection(remoteEntries);
        });
    }

    private List<Cache.Entry<K, V>> getRemoteServersEntries() {
        return remoteServersStream()
                .map(this::getRemoteEntries)
                .flatMap(Collection::stream)
                .toList();
    }

    private Stream<RemotePortunusServer> remoteServersStream() {
        return discoveryService.remoteServers().parallelStream();
    }

    private Set<Cache.Entry<K, V>> getRemoteEntries(PortunusServer remoteServer) {
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

    private void putEntry(K key, Entry<K, V> entry) {
        PortunusServer owner;

        if (isLocalPartition(key)) {
            owner = discoveryService.localServer();
        } else {
            owner = partitionService.getPartitionForKey(key).owner();
        }
        putEntry(owner, entry);
    }

    @SneakyThrows
    private void putEntry(PortunusServer owner, Entry<K, V> entry) {
        owner.put(name, entry);
    }

    @SneakyThrows
    private boolean isLocalPartition(K key) {
        return partitionService.isLocalPartition(key);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        executeOperation(OperationType.PUT_ALL, () -> {
            validate(entries);
            entries.forEach((key, value) -> {
                Entry<K, V> entry = new Entry<>(key, value);
                cacheEntryObserver.onAdd(entry);
                PortunusServer owner = discoveryService.localServer();
                putEntry(owner, entry);
            });
            return entries;
        });
    }

    private void validate(Map<K, V> entries) {
        entries.forEach(this::validate);
    }

    @Override
    public Cache.Entry<K, V> remove(K key) {
        return executeOperation(OperationType.REMOVE, () ->
                Optional.ofNullable(removeEntry(key))
                        .map(it -> {
                            cacheEntryObserver.onRemove(it);
                            return it;
                        })
                        .orElseThrow(() -> new PortunusException("Entry is not present"))
        );
    }

    @SneakyThrows
    private Cache.Entry<K, V> removeEntry(K key) {
        return discoveryService.localServer().remove(name, key);
    }

    @Override
    public void removeAll(Collection<K> keys) {
        executeOperation(OperationType.REMOVE_ALL, () -> {
            keys.forEach(this::remove);
            return keys;
        });
    }

    private void validate(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    private <T> T executeOperation(OperationType operationType, Callable<T> operation) {
        try {
            log.debug("Executing operation: '{}'", operationType.name());
            Future<T> futureResult = operationExecutor.submit(operation);
            return futureResult.get();
        } catch (Exception e) {
            log.error("Failed to execute operation: '{}'", operationType.name());
            throw new OperationFailedException(String.format("Operation: '%s' failed", operationType.name()));
        }
    }

    @Override
    public void shutdown() {
        operationExecutor.shutdown();
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
    }

    private enum OperationType {
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET_ENTRY,
        GET_ENTRIES,
        GET_ALL_ENTRIES,
        PUT,
        PUT_ALL,
        REMOVE,
        REMOVE_ALL
    }
}
