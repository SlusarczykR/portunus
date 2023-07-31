package org.slusarczykr.portunus.cache.cluster.chunk;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;

import java.io.Serializable;
import java.util.Set;

public record CacheChunk(Partition partition, Set<Cache<? extends Serializable, ? extends Serializable>> cacheEntries) {

    public int getPartitionId() {
        return partition.getPartitionId();
    }
}
