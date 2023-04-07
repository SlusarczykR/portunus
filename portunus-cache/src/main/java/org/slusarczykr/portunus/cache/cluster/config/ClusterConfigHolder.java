package org.slusarczykr.portunus.cache.cluster.config;

public interface ClusterConfigHolder {

    ClusterConfig getClusterConfig();

    void overrideClusterConfig(ClusterConfig clusterConfig);
}
