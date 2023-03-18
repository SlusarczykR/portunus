package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PortunusHashingCircle {

    void add(Address address) throws PortunusException;

    void remove(Address address) throws PortunusException;
}
