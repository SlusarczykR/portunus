package org.slusarczykr.portunus.cache.cluster.partition.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DefaultMigrationService extends AbstractService implements MigrationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultMigrationService.class);

    public static DefaultMigrationService newInstance(ClusterService clusterService) {
        return new DefaultMigrationService(clusterService);
    }

    protected DefaultMigrationService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void migrate(Collection<Partition> partitions, Address address, boolean replicate) {
        try {
            log.debug("Migrating {} partitions to: '{}'", partitions.size(), address);
            RemotePortunusServer remotePortunusServer = (RemotePortunusServer) clusterService.getDiscoveryService().getServer(address);
            List<CacheChunk> cacheChunks = getCacheChunks(partitions);

            CompletableFuture.runAsync(() -> remotePortunusServer.migrate(cacheChunks, replicate));

            removeLocalCachesEntries(partitions);
        } catch (PortunusException e) {
            log.error(String.format("Could not migrate partitions to: '%s'", address), e);
        }
    }

    private void removeLocalCachesEntries(Collection<Partition> partitions) {
        log.debug("Removing local caches entries");
        partitions.forEach(it -> clusterService.getLocalServer().remove(it));
    }

    private List<CacheChunk> getCacheChunks(Collection<Partition> partitions) {
        return partitions.stream()
                .map(it -> clusterService.getLocalServer().getCacheChunk(it))
                .toList();
    }

    @Override
    public void migratePartitionReplicas(Collection<Partition> partitions) {
        log.debug("Recreating {} partitions from local replicas", partitions.size());
        List<CacheChunk> cacheChunks = getCacheChunks(partitions);
        cacheChunks.forEach(it -> migrateToLocalServer(it, true));
    }

    @Override
    public void migrateToLocalServer(Collection<CacheChunk> cacheChunk, boolean replicate) {
        cacheChunk.forEach(it -> migrateToLocalServer(it, replicate));
    }

    @Override
    public void migrateToLocalServer(CacheChunk cacheChunk, boolean replicate) {
        log.debug("Migrating partition [{}] to local server - replica: {}", cacheChunk.partition().getPartitionId(), replicate);
        logCacheChunkDetails(cacheChunk);
        Partition partition = reassignOwner(cacheChunk.partition());
        log.debug("Reassigned partition: {}", partition);

        clusterService.getPartitionService().register(partition);

        updateLocalCache(partition, cacheChunk, replicate);
        log.debug("Successfully migrated partition: {}", partition);
    }

    private void updateLocalCache(Partition partition, CacheChunk cacheChunk, boolean replicate) {
        CacheChunk reassignedCacheChunk = new CacheChunk(partition, cacheChunk.cacheEntries());
        clusterService.getLocalServer().update(reassignedCacheChunk);

        if (replicate) {
            clusterService.getReplicaService().replicatePartition(partition);
        }
    }

    private Partition reassignOwner(Partition partition) {
        LocalPortunusServer localServer = clusterService.getLocalServer();
        Set<Address> replicaOwners = partition.getReplicaOwners().stream()
                .filter(it -> !it.equals(localServer.getAddress()))
                .collect(Collectors.toSet());

        return new Partition(partition.getPartitionId(), localServer, replicaOwners);
    }

    private void logCacheChunkDetails(CacheChunk cacheChunk) {
        if (log.isTraceEnabled()) {
            cacheChunk.cacheEntries().forEach(cache ->
                    cache.allEntries().forEach(it -> {
                                int partitionId = cacheChunk.getPartitionId();
                                log.trace("Cache chunk entry '{}' from '{}' [{}]", it.getKey(), cache.getName(), partitionId);
                            }
                    )
            );
        }
    }

    @Override
    public String getName() {
        return MigrationService.class.getSimpleName();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
