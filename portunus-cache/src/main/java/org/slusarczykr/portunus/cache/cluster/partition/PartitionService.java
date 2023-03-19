package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PartitionService {

    boolean isLocalPartition(String key) throws PortunusException;

    int getPartitionId(String key);

    Partition getPartition(String key);

    Address getPartitionOwner(String key) throws PortunusException;

    void register(Address address) throws PortunusException;

    void unregister(Address address) throws PortunusException;
}
