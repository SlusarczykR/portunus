package org.slusarczykr.portunus.cache.cluster.partition.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractConcurrentService;

import java.util.Map;
import java.util.Optional;
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
        log.debug("Registering partition replica [{}]", partition.getPartitionId());
        log.trace("Partition replica: {}", partition);
        withWriteLock(() -> partitionReplicas.put(partition.getPartitionId(), partition));
    }

    @Override
    public void unregisterPartitionReplica(int partitionId) {
        log.debug("Unregistering partition: {}", partitionId);
        withWriteLock(() -> partitionReplicas.remove(partitionId));
    }

    @Override
    public void updatePartitionReplica(Partition partition) {
        partition.getReplicaOwners().stream()
                .map(it -> clusterService.getDiscoveryService().getServer(it))
                .flatMap(Optional::stream)
                .forEach(it -> it.replicate(partition));
    }

    @Override
    public void replicatePartition(Partition partition) {
        Map<PortunusServer, Long> ownerToPartitionCount = clusterService.getPartitionService().getOwnerPartitionsCount();
        log.trace("Owner to partition count: {}", ownerToPartitionCount.entrySet());
        Optional<PortunusServer> remoteServer = getRemoteServerByPartitionsCount(ownerToPartitionCount);

        remoteServer.ifPresent(it -> replicate(partition, (RemotePortunusServer) it));
    }

    private static Optional<PortunusServer> getRemoteServerByPartitionsCount(Map<PortunusServer, Long> ownerToPartitionCount) {
        return ownerToPartitionCount.entrySet().stream()
                .filter(it -> !it.getKey().isLocal())
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .findFirst();
    }

    private void replicate(Partition partition, RemotePortunusServer portunusServer) {
        log.debug("Replicating partition [{}] on server: '{}'", partition.getPartitionId(), portunusServer.getPlainAddress());
        portunusServer.replicate(partition);
        log.trace("Replicated partition: {}", partition);
    }

    @Override
    public String getName() {
        return ReplicaService.class.getSimpleName();
    }
}
