package org.slusarczykr.portunus.cache.cluster.replica;

import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.service.Service;

public interface ReplicaService extends Service {

    boolean isReplicaOwner(int partitionId);

    boolean isReplicaOwnerForKey(Object key);

    void registerReplica(Partition partition);

    void unregisterReplica(int partitionId);
}
