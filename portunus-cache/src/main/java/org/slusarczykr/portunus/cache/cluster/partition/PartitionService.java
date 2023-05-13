package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public interface PartitionService extends Service {

    SortedMap<String, VirtualPortunusNode> getPartitionOwnerCircle();

    boolean isLocalPartition(Object key) throws PortunusException;

    int getPartitionId(Object key);

    Partition register(Partition partition);

    Partition getPartition(int partitionId);

    Partition getPartitionForKey(Object key);

    Partition getLocalPartition(int partitionId) throws PortunusException;

    Map<Integer, Partition> getPartitionMap();

    List<Partition> getLocalPartitions();

    Address getPartitionOwner(String key) throws PortunusException;

    void register(PortunusServer server) throws PortunusException;

    void unregister(Address address) throws PortunusException;

    List<String> getRegisteredAddresses();

    void update(SortedMap<String, VirtualPortunusNode> virtualPortunusNodes,
                Map<Integer, Partition> partitions);

    Map<PortunusServer, Long> getOwnerPartitionsCount();
}
