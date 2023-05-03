package org.slusarczykr.portunus.cache.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDistributedCacheManager extends DefaultCacheManager implements DistributedCacheManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultDistributedCacheManager.class);

    private final Map<Integer, Set<Cache<? extends Serializable, ? extends Serializable>>> partitionIdToCache = new ConcurrentHashMap<>();

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        return partitionIdToCache.computeIfAbsent(partitionId, ConcurrentHashMap::newKeySet);
    }
}
