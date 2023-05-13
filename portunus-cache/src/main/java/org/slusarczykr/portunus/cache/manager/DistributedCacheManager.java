package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface DistributedCacheManager extends CacheManager {

    boolean anyCacheEntry(int partitionId);

    Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId);

    <K extends Serializable, V extends Serializable> void register(int partitionId, String name,
                                                                   Map<K, V> cacheEntries);

    <K extends Serializable, V extends Serializable> void register(int partitionId, String name,
                                                                   Set<Cache.Entry<K, V>> cacheEntries);

    void remove(int partitionId);
}
