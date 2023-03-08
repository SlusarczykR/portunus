package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.config.CacheConfig;

import java.util.Collection;

public interface CacheManager {

    <K, V> Cache<K, V> getCache(String name);

    Collection<Cache<?, ?>> allCaches();

    <K, V> Cache<K, V> newCache(String name, CacheConfig<K, V> configuration);

    void removeCache(String name);
}
