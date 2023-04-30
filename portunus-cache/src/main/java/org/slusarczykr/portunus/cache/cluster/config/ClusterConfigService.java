package org.slusarczykr.portunus.cache.cluster.config;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;

public interface ClusterConfigService extends ClusterConfigHolder, Service {

    void overrideClusterConfig(ClusterConfig clusterConfig);

    String getLocalServerPlainAddress() throws PortunusException;

    Address getLocalServerAddress() throws PortunusException;

    ClusterConfig getClusterConfig();

    boolean isMulticastEnabled();

    List<Address> getClusterMembers();

    int getNumberOfClusterMembers();
}
