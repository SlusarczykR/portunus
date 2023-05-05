package org.slusarczykr.portunus.cache.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.config.CacheConfig;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCacheManager implements CacheManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultDistributedCacheManager.class);

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    @Override
    public <K, V> Cache<K, V> getCache(String name) {
        return newCache(name);
    }

    @Override
    public Collection<Cache<?, ?>> allCaches() {
        return caches.values();
    }

    private <K, V> Cache<K, V> newCache(String name) {
        return newCache(name, new HashMap<>());
    }

    @Override
    public <K, V> Cache<K, V> newCache(String name, CacheConfig<K, V> configuration) {
        return newCache(name, configuration.getEventListeners());
    }

    private <K, V> Cache<K, V> newCache(String name, Map<CacheEventType, CacheEventListener> eventListeners) {
        return (Cache<K, V>) caches.computeIfAbsent(name, it -> new DefaultCache<>(name, eventListeners));
    }

    @Override
    public void removeCache(String name) {
        caches.remove(name);
    }
}
