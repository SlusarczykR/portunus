package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PartitionService {

    Partition getPartition(String key);

    String getPartitionOwner(String key) throws PortunusException;

    void register(Address address) throws PortunusException;

    void unregister(Address address) throws PortunusException;
}
