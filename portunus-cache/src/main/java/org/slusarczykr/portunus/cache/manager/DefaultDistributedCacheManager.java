package org.slusarczykr.portunus.cache.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultDistributedCacheManager extends DefaultCacheManager implements DistributedCacheManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultDistributedCacheManager.class);

    private final Map<Integer, Set<Cache<? extends Serializable, ? extends Serializable>>> partitionIdToCache = new ConcurrentHashMap<>();

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        return partitionIdToCache.computeIfAbsent(partitionId, it -> ConcurrentHashMap.newKeySet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> void register(int partitionId, String name, Map<K, V> cacheEntries) {
        Set<Cache.Entry<K, V>> entries = cacheEntries.entrySet().stream()
                .map(it -> new DefaultCache.Entry<>(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());

        register(partitionId, name, entries);
    }

    @Override
    public <K extends Serializable, V extends Serializable> void register(int partitionId, String name,
                                                                          Set<Cache.Entry<K, V>> cacheEntries) {
        Set<Cache<? extends Serializable, ? extends Serializable>> caches = getCacheEntries(partitionId);

        Cache<K, V> cache = (Cache<K, V>) caches.stream()
                .filter(it -> it.getName().equals(name))
                .findFirst()
                .orElseGet(() -> newCache(caches, name));

        cache.putAll(cacheEntries);
    }

    private <K extends Serializable, V extends Serializable> Cache<K, V> newCache(Set<Cache<? extends Serializable, ? extends Serializable>> caches,
                                                                                  String name) {
        Cache<K, V> cache = new DefaultCache<>(name);
        caches.add(cache);

        return cache;
    }
}
