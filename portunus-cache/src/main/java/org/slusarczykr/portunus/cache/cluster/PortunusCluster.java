package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;

import java.io.Serializable;

public interface PortunusCluster {

    static PortunusCluster newInstance() {
        return PortunusClusterInstance.getInstance(null);
    }

    static PortunusCluster newInstance(ClusterConfig clusterConfig) {
        return PortunusClusterInstance.getInstance(clusterConfig);
    }

    <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name);

    void shutdown();
}
