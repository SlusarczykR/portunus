package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;

import java.io.Serializable;
import java.util.List;

public interface PortunusCluster {

    <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name);

    LocalPortunusServer localMember();

    List<RemotePortunusServer> remoteMembers();

    static PortunusCluster newInstance(ClusterConfig clusterConfig) {
        return PortunusClusterInstance.getInstance(clusterConfig);
    }
}
