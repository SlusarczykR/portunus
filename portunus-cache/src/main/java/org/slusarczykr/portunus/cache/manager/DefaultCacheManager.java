package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.config.CacheConfig;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCacheManager implements CacheManager {

    private static final DefaultCacheManager INSTANCE = new DefaultCacheManager();

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    public static DefaultCacheManager getInstance() {
        return INSTANCE;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String name) {
        return (Optional.ofNullable((Cache<K, V>) caches.get(name))
                .orElseGet(() -> newCache(name)));
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
        return (Cache<K, V>) caches.computeIfAbsent(name, it -> new DefaultCache<>(eventListeners));
    }

    @Override
    public void removeCache(String name) {
        caches.remove(name);
    }
}
