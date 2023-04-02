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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class DistributedCache<K extends Serializable, V extends Serializable> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DistributedCache.class);

    private final PartitionService partitionService;
    private final DiscoveryService discoveryService;

    private final String name;
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DistributedCache(String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        this.name = name;
        this.partitionService = DefaultPartitionService.getInstance();
        this.discoveryService = DefaultDiscoveryService.getInstance();
        eventListeners.forEach(cacheEntryObserver::register);
    }


    @Override
    public boolean isEmpty() {
        boolean anyLocalEntry = discoveryService.localServer().anyEntry(name);
        boolean anyEntry = anyLocalEntry || anyRemoteEntry();

        return !anyEntry;
    }

    private boolean anyRemoteEntry() {
        return remoteServersStream()
                .anyMatch(it -> it.anyEntry(name));
    }

    @Override
    public boolean containsKey(K key) throws PortunusException {
        String objectKey = key.toString();

        if (!partitionService.isLocalPartition(objectKey)) {
            Partition partition = partitionService.getPartitionForKey(objectKey);
            return partition.owner().containsEntry(name, key);
        }
        return discoveryService.localServer().containsEntry(name, key);
    }

    @Override
    public boolean containsValue(V value) {
        return discoveryService.localServer().getCacheEntries(name).stream()
                .anyMatch(it -> it.getValue().equals(value));
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
        return keys.stream()
                .map(this::getEntry)
                .map(Optional::stream)
                .map(it -> (Cache.Entry<K, V>) it)
                .toList();

    }

    @Override
    public Collection<Cache.Entry<K, V>> allEntries() {
        List<Cache.Entry<K, V>> remoteEntries = new ArrayList<>(getRemoteServersEntries());
        Set<Cache.Entry<K, V>> entries = discoveryService.localServer().getCacheEntries(name);
        entries.forEach(cacheEntryObserver::onAccess);
        remoteEntries.addAll(entries);

        return Collections.unmodifiableCollection(remoteEntries);
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
        withErrorHandling(() -> {
            validate(key, value);
            Entry<K, V> entry = new Entry<>(key, value);
            putEntry(key, entry);
            cacheEntryObserver.onAdd(entry);
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

    private void withErrorHandling(Runnable executable) {
        try {
            executable.run();
        } catch (Exception e) {
            throw new OperationFailedException("Could not execute operation");
        }
    }

    @Override
    public void putAll(Map<K, V> entries) {
        entries.forEach(this::validate);

        entries.forEach((key, value) -> {
            Entry<K, V> entry = new Entry<>(key, value);
            cacheEntryObserver.onAdd(entry);
            PortunusServer owner = discoveryService.localServer();
            putEntry(owner, entry);
        });
    }

    @Override
    public Cache.Entry<K, V> remove(K key) throws PortunusException {
        return Optional.ofNullable(removeEntry(key))
                .map(it -> {
                    cacheEntryObserver.onRemove(it);
                    return it;
                })
                .orElseThrow(() -> new PortunusException("Entry is not present"));
    }

    @SneakyThrows
    private Cache.Entry<K, V> removeEntry(K key) {
        return discoveryService.localServer().remove(name, key);
    }

    @Override
    public void removeAll(Collection<K> keys) {
        keys.forEach(it -> {
            try {
                remove(it);
            } catch (PortunusException e) {
                log.error("Could not remove entry", e);
            }
        });
    }

    private void validate(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
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
}
