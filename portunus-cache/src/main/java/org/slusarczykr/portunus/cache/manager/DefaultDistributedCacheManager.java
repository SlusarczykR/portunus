package org.slusarczykr.portunus.cache.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultDistributedCacheManager extends DefaultCacheManager implements DistributedCacheManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultDistributedCacheManager.class);

    private final Map<Integer, Set<Cache<? extends Serializable, ? extends Serializable>>> partitionIdToCache = new ConcurrentHashMap<>();

    @Override
    public boolean anyCacheEntry(int partitionId) {
        return partitionIdToCache.containsKey(partitionId);
    }

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        return partitionIdToCache.computeIfAbsent(partitionId, it -> ConcurrentHashMap.newKeySet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> void register(int partitionId, String name, Map<K, V> cacheEntries) {
        Set<Cache.Entry<K, V>> entries = toEntrySet(cacheEntries);
        register(partitionId, name, entries);
    }

    @Override
    public <K extends Serializable, V extends Serializable> void register(int partitionId, String name,
                                                                          Set<Cache.Entry<K, V>> cacheEntries) {
        Cache<K, V> cache = getCache(partitionId, name);
        cache.putAll(cacheEntries);
    }

    @Override
    public <K extends Serializable> void unregister(int partitionId, String name, Set<K> keys) {
        Cache<K, ? extends Serializable> cache = getCache(partitionId, name);
        cache.removeAll(keys);
    }

    @Override
    public void remove(int partitionId) {
        Set<Cache<? extends Serializable, ? extends Serializable>> caches = getCacheEntries(partitionId);
        caches.forEach(this::removeCacheChunk);
        partitionIdToCache.remove(partitionId);
    }

    private <K extends Serializable, V extends Serializable> void removeCacheChunk(Cache<K, V> cacheChunk) {
        Cache<K, V> cache = getCache(cacheChunk.getName());
        List<K> keys = cacheChunk.allEntries().stream()
                .map(Cache.Entry::getKey)
                .toList();

        cache.removeAll(keys);
    }

    private <K extends Serializable, V extends Serializable> Cache<K, V> getCache(int partitionId, String name) {
        Set<Cache<? extends Serializable, ? extends Serializable>> caches = getCacheEntries(partitionId);
        return getOrCreate(caches, name);
    }

    private <K extends Serializable, V extends Serializable> Cache<K, V> getOrCreate(Set<Cache<? extends Serializable, ? extends Serializable>> caches, String name) {
        return (Cache<K, V>) caches.stream()
                .filter(it -> it.getName().equals(name))
                .findFirst()
                .orElseGet(() -> newCache(caches, name));
    }

    private <K extends Serializable, V extends Serializable> Cache<K, V> newCache(Set<Cache<? extends Serializable, ? extends Serializable>> caches,
                                                                                  String name) {
        Cache<K, V> cache = new DefaultCache<>(name);
        caches.add(cache);

        return cache;
    }

    private <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> toEntrySet(Map<K, V> cacheEntries) {
        return cacheEntries.entrySet().stream()
                .map(it -> new DefaultCache.Entry<>(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());
    }
}
