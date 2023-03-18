package org.slusarczykr.portunus.cache.cluster.client;


import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;

import java.util.Collection;

public interface PortunusClient {

    <K, V> Cache<K, V> getCache();

    Collection<Partition> getPartitions();
}
