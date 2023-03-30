package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;

public interface PartitionService extends Service {

    boolean isLocalPartition(Object key) throws PortunusException;

    int getPartitionId(Object key);

    Partition getPartition(int partitionId);

    Partition getPartitionForKey(Object key);

    Partition getLocalPartition(int partitionId) throws PortunusException;

    List<Partition> getLocalPartitions();

    Address getPartitionOwner(String key) throws PortunusException;

    void register(Address address) throws PortunusException;

    void unregister(Address address) throws PortunusException;
}
