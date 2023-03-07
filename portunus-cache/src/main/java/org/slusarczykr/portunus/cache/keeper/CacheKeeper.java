package org.slusarczykr.portunus.cache.keeper;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.config.CacheConfig;

import java.util.Collection;

public interface CacheKeeper {

    <K, V> Cache<K, V> getCache(String name);

    Collection<Cache<?, ?>> allCaches();

    <K, V> Cache<K, V> newCache(String name, CacheConfig<K, V> configuration);
}
