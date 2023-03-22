package org.slusarczykr.portunus.cache.cluster.client;


import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntry;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface PortunusClient {

    <K extends Serializable> boolean containsEntry(String cacheName, K key);

    Set<CacheEntry> getCache(String name);

    Collection<Partition> getPartitions();
}
