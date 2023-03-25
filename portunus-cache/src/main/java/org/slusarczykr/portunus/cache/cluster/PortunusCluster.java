package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

import java.util.List;

public interface PortunusCluster {

    PortunusServer localMember();

    List<PortunusServer> allMembers();

    static PortunusCluster newInstance() {
        return PortunusClusterInstance.newInstance();
    }
}
