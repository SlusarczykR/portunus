package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntry;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;

import java.io.Serializable;

public interface ConversionService {

    Partition convert(PortunusApiProtos.Partition partition);

    PortunusApiProtos.Partition convert(Partition partition);

    <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntry cacheEntry);

    <K extends Serializable, V extends Serializable> CacheEntry convert(Cache.Entry<K, V> cacheEntry);
}
