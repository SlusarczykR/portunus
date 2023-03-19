package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.config.CacheConfig;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCacheManager implements CacheManager {

    private static final DefaultCacheManager INSTANCE = new DefaultCacheManager();

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    public static DefaultCacheManager getInstance() {
        return INSTANCE;
    }

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
