package org.slusarczykr.portunus.cache;

import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;
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
import java.util.concurrent.ConcurrentHashMap;

public class DistributedCache<K extends Serializable, V> implements Cache<K, V> {

    private final PartitionService partitionService;
    private final DiscoveryService discoveryService;

    private final String name;
    private final Map<K, Cache.Entry<K, V>> cache = new ConcurrentHashMap<>();
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DistributedCache(String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        this.name = name;
        this.partitionService = DefaultPartitionService.getInstance();
        this.discoveryService = DefaultDiscoveryService.getInstance();
        eventListeners.forEach(cacheEntryObserver::register);
    }


    @Override
    public boolean containsKey(K key) throws PortunusException {
        String objectKey = key.toString();

        if (!partitionService.isLocalPartition(objectKey)) {
            Partition partition = partitionService.getPartitionForKey(objectKey);
            return partition.owner().containsEntry(name, key);
        }
        return cache.containsKey(key);
    }

    @Override
    public boolean containsValue(V value) {
        return cache.entrySet().stream()
                .anyMatch(it -> it.getValue().equals(value));
    }

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return Optional.ofNullable(cache.get(key))
                .map(it -> {
                    cacheEntryObserver.onAccess(it);
                    return it;
                });
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
        List<Cache.Entry<K, V>> remoteEntries = new ArrayList<>(getRemoteEntries());
        Collection<Cache.Entry<K, V>> entries = cache.values();
        entries.forEach(cacheEntryObserver::onAccess);
        remoteEntries.addAll(entries);

        return Collections.unmodifiableCollection(remoteEntries);
    }

    private List<Cache.Entry<K, V>> getRemoteEntries() {
        return discoveryService.remoteServers().stream()
                .map(this::getRemoteEntries)
                .flatMap(Collection::stream)
                .toList();
    }

    private Set<Cache.Entry<K, V>> getRemoteEntries(PortunusServer remoteServer) {
        return remoteServer.getCacheEntries(name);
    }

    @Override
    public void put(K key, V value) {
        validate(key, value);
        Entry<K, V> entry = new Entry<>(key, value);
        cache.put(key, entry);
        cacheEntryObserver.onAdd(entry);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        entries.forEach(this::validate);
        entries.forEach((key, value) -> {
            Entry<K, V> entry = new Entry<>(key, value);
            cacheEntryObserver.onAdd(entry);
            cache.put(key, entry);
        });
    }

    @Override
    public void remove(K key) {
        Optional.ofNullable(cache.remove(key)).ifPresent(cacheEntryObserver::onRemove);
    }

    @Override
    public void removeAll(Collection<K> keys) {
        keys.forEach(this::remove);
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
