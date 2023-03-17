package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig;

public interface PortunusServer {

    String getAddress();

    <K, V> Cache.Entry<K, V> getEntry(String key);

    record ClusterMemberContext(ClusterDiscoveryConfig clusterDiscoveryConfig, String address) {
    }
}
