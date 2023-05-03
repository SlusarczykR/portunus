package org.slusarczykr.portunus.cache.cluster.partition.migration;

import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

import java.util.Collection;

public interface MigrationService {

    void migrate(Collection<Partition> partitions, PortunusServer portunusServer);
}
