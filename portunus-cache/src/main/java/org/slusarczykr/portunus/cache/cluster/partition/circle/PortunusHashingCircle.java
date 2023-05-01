package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Set;
import java.util.SortedMap;

public interface PortunusHashingCircle {

    SortedMap<String, VirtualPortunusNode> get();

    boolean isEmpty();

    int getSize();

    Set<String> getKeys();

    Set<String> getAddresses();

    void update(SortedMap<String, VirtualPortunusNode> virtualPortunusNodes);

    void add(Address address) throws PortunusException;

    void remove(Address address) throws PortunusException;
}
