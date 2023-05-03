package org.slusarczykr.portunus.cache.cluster.replica;

import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.service.Service;

public interface ReplicaService extends Service {

    boolean isPartitionReplicaOwner(int partitionId);

    boolean isPartitionReplicaOwnerForKey(Object key);

    void registerPartitionReplica(Partition partition);

    void unregisterPartitionReplica(int partitionId);

    void replicatePartition(Partition partition);
}
