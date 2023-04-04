package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Set;

public interface PortunusHashingCircle {

    boolean isEmpty();

    int getSize();

    Set<String> getKeys();

    Set<String> getAddresses();

    void add(Address address) throws PortunusException;

    void remove(Address address) throws PortunusException;
}
