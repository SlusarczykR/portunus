package org.slusarczykr.portunus.cache.cluster.config;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

import java.util.List;

public interface ClusterConfigService {

    ClusterConfig getClusterConfig();

    List<Address> getClusterMembers();
}
