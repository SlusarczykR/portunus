package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;

import java.util.List;

public interface PortunusCluster {

    PortunusServer localMember();

    List<RemotePortunusServer> remoteMembers();

    void onShutdown();

    static PortunusCluster newInstance() {
        return PortunusClusterInstance.newInstance();
    }
}
