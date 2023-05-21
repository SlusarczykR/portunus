package org.slusarczykr.portunus.cache.cluster.partition.migration;

import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.Service;

import java.util.Collection;

public interface MigrationService extends Service {

    void migrate(Collection<Partition> partitions, Address address);

    void migratePartitionReplicas(Collection<Partition> partitions);

    void migrateToLocalServer(CacheChunk cacheChunk);

    void migrateToLocalServer(Collection<CacheChunk> cacheChunk);
}
