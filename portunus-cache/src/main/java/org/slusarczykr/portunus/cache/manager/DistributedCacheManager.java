package org.slusarczykr.portunus.cache.manager;

import org.slusarczykr.portunus.cache.Cache;

import java.io.Serializable;
import java.util.Set;

public interface DistributedCacheManager extends CacheManager {

    Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId);
}
