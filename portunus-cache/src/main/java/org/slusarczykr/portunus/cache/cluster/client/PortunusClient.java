package org.slusarczykr.portunus.cache.cluster.client;

import org.slusarczykr.portunus.cache.cluster.partition.Partition;

public interface PortunusClient {

    Partition getPartitions();
}
