package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.config.CacheConfig;
import org.slusarczykr.portunus.cache.impl.DefaultCache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCacheManager implements CacheManager {

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    @Override
    public <K, V> Cache<K, V> getCache(String name) {
        return (Cache<K, V>) caches.get(name);
    }

    @Override
    public Collection<Cache<?, ?>> allCaches() {
        return caches.values();
    }

    @Override
    public <K, V> Cache<K, V> newCache(String name, CacheConfig<K, V> configuration) {
        return new DefaultCache<>(configuration.getEventListeners());
    }

    @Override
    public void removeCache(String name) {
        caches.remove(name);
    }
}
