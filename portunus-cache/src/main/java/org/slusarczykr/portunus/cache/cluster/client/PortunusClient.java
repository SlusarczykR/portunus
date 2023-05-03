package org.slusarczykr.portunus.cache.cluster.client;


import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface PortunusClient {

    boolean anyEntry(String cacheName);

    <K extends Serializable> boolean containsEntry(String cacheName, K key);

    Set<CacheEntryDTO> getCache(String name);

    <K extends Serializable> CacheEntryDTO getCacheEntry(String name, K key);

    Collection<PartitionDTO> getPartitions();

    void sendEvent(ClusterEvent event);

    void sendEvent(PartitionEvent event);

    boolean putEntry(String cacheName, CacheEntryDTO entry);

    <K extends Serializable> CacheEntryDTO removeEntry(String cacheName, K key);

    boolean replicate(PartitionDTO partition);
}
