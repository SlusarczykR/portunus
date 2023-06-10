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

public class DefaultMigrationService extends AbstractService implements MigrationService {

    private static final Logger log = LoggerFactory.getLogger(DefaultMigrationService.class);

    public static DefaultMigrationService newInstance(ClusterService clusterService) {
        return new DefaultMigrationService(clusterService);
    }

    protected DefaultMigrationService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void migrate(Collection<Partition> partitions, Address address) {
        try {
            log.info("Start migration of {} partitions to server: '{}'", partitions.size(), address);
            RemotePortunusServer remotePortunusServer = (RemotePortunusServer) clusterService.getDiscoveryService().getServerOrThrow(address);
            List<CacheChunk> cacheChunks = getCacheChunks(partitions);

            remotePortunusServer.migrate(cacheChunks);

            removeLocalCachesEntries(partitions);
        } catch (PortunusException e) {
            log.error(String.format("Could not migrate partitions to the server: '%s'", address), e);
        }
    }

    private void removeLocalCachesEntries(Collection<Partition> partitions) {
        log.info("Removing local caches entries");
        partitions.forEach(it -> clusterService.getLocalServer().remove(it));
    }

    private List<CacheChunk> getCacheChunks(Collection<Partition> partitions) {
        return partitions.stream()
                .map(it -> clusterService.getLocalServer().getCacheChunk(it))
                .toList();
    }

    @Override
    public void migratePartitionReplicas(Collection<Partition> partitions) {
        log.info("Start migration partitions to local server: '{}'", partitions.size());
        List<CacheChunk> cacheChunks = getCacheChunks(partitions);
        cacheChunks.forEach(this::migrateToLocalServer);
    }

    @Override
    public void migrateToLocalServer(Collection<CacheChunk> cacheChunk) {
        cacheChunk.forEach(this::migrateToLocalServer);
    }

    @Override
    public void migrateToLocalServer(CacheChunk cacheChunk) {
        log.info("Start migrating partition: {}", cacheChunk.partition());
        Partition partition = reassignOwner(cacheChunk.partition());

        clusterService.getPartitionService().register(partition);
        CacheChunk reassignedCacheChunk = new CacheChunk(partition, cacheChunk.cacheEntries());
        clusterService.getLocalServer().update(reassignedCacheChunk);
        log.info("Successfully migrated partition: {}", partition);
    }

    private Partition reassignOwner(Partition partition) {
        LocalPortunusServer localServer = clusterService.getLocalServer();
        return new Partition(partition.getPartitionId(), localServer, partition.getReplicaOwners());
    }

    @Override
    public String getName() {
        return MigrationService.class.getSimpleName();
    }
}
