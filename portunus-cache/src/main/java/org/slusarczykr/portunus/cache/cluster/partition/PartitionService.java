package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

public interface PartitionService {

    PortunusServer getPartitionOwner(String key);
}
