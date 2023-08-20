package org.slusarczykr.portunus.cache.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.config.CacheConfig;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCacheManager implements CacheManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultCacheManager.class);

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    @Override
    public <K, V> Cache<K, V> getCache(String name) {
        return getCache(name, Map.of(), false);
    }

    @Override
    public Collection<Cache<?, ?>> allCaches() {
        return caches.values();
    }

    @Override
    public <K, V> Cache<K, V> newCache(String name, CacheConfig<K, V> configuration) {
        return getCache(name, configuration.getEventListeners(), true);
    }

    private <K, V> Cache<K, V> getCache(String name, Map<CacheEventType, CacheEventListener> eventListeners, boolean create) {
        if (create) {
            log.debug("Creating cache with name: '{}'", name);
            return (Cache<K, V>) caches.put(name, new DefaultCache<>(name, eventListeners));
        }
        log.debug("Getting cache with name: '{}'", name);
        return (Cache<K, V>) caches.computeIfAbsent(name, it -> new DefaultCache<>(name, eventListeners));
    }

    @Override
    public void removeCache(String name) {
        caches.remove(name);
    }
}
