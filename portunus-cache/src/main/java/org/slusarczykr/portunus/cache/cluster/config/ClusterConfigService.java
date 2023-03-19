package org.slusarczykr.portunus.cache.cluster.config;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;

public interface ClusterConfigService {

    String getLocalServerPlainAddress() throws PortunusException;

    Address getLocalServerAddress() throws PortunusException;

    ClusterConfig getClusterConfig();

    List<Address> getClusterMembers();
}
