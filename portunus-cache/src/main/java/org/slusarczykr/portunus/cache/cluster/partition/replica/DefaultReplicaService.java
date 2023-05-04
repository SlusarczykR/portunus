package org.slusarczykr.portunus.cache.cluster.partition.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
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
    public boolean isPartitionReplicaOwner(int partitionId) {
        return withReadLock(() -> partitionReplicas.containsKey(partitionId));
    }

    @Override
    public boolean isPartitionReplicaOwnerForKey(Object key) {
        return withReadLock(() -> {
            int partitionId = clusterService.getPartitionService().getPartitionId(key);
            return partitionReplicas.containsKey(partitionId);
        });
    }

    @Override
    public void registerPartitionReplica(Partition partition) {
        log.info("Registering partition: {} replica for server: '{}'", partition.getPartitionId(), partition.getOwnerPlainAddress());
        withWriteLock(() -> partitionReplicas.computeIfAbsent(partition.getPartitionId(), it -> partition));
    }

    @Override
    public void unregisterPartitionReplica(int partitionId) {
        log.info("Unregistering partition: {}", partitionId);
        withWriteLock(() -> partitionReplicas.remove(partitionId));
    }

    @Override
    public void updatePartitionReplica(Partition partition) {
        partition.getReplicaOwners().forEach(it -> it.replicate(partition));
    }

    @Override
    public void replicatePartition(Partition partition) {
        Map<PortunusServer, Long> portunusServersCount = clusterService.getPartitionService().getPartitionsCount();

        portunusServersCount.entrySet().stream()
                .filter(it -> !it.getKey().isLocal())
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .findFirst()
                .ifPresent(it -> replicate(partition, (RemotePortunusServer) it));
    }

    private void replicate(Partition partition, RemotePortunusServer portunusServer) {
        portunusServer.replicate(partition);
        partition.addReplicaOwner(portunusServer);
    }

    @Override
    public String getName() {
        return ReplicaService.class.getSimpleName();
    }
}
