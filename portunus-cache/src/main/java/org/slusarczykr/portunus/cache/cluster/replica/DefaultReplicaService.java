package org.slusarczykr.portunus.cache.cluster.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.service.AbstractConcurrentService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultReplicaService extends AbstractConcurrentService implements ReplicaService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReplicaService.class);

    private final Map<Integer, Partition> partitionReplicas = new ConcurrentHashMap<>();

    public static DefaultReplicaService newInstance(ClusterService clusterService) {
        return new DefaultReplicaService(clusterService);
    }

    private DefaultReplicaService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public boolean isReplicaOwner(int partitionId) {
        return withReadLock(() -> partitionReplicas.containsKey(partitionId));
    }

    @Override
    public boolean isReplicaOwnerForKey(Object key) {
        return withReadLock(() -> {
            int partitionId = clusterService.getPartitionService().getPartitionId(key);
            return partitionReplicas.containsKey(partitionId);
        });
    }

    @Override
    public void registerReplica(Partition partition) {
        withWriteLock(() -> partitionReplicas.computeIfAbsent(partition.partitionId(), it -> partition));
    }

    @Override
    public void unregisterReplica(int partitionId) {
        withWriteLock(() -> partitionReplicas.remove(partitionId));
    }

    @Override
    public String getName() {
        return ReplicaService.class.getSimpleName();
    }
}
