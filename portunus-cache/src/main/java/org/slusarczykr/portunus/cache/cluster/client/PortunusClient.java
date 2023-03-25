package org.slusarczykr.portunus.cache.cluster.client;


import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface PortunusClient {

    <K extends Serializable> boolean containsEntry(String cacheName, K key);

    Set<CacheEntryDTO> getCache(String name);

    Collection<PartitionDTO> getPartitions();

    void sendEvent(ClusterEvent event);
}
