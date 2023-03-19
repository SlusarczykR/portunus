package org.slusarczykr.portunus.cache.cluster.partition.strategy;

import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PartitionKeyStrategy {

    String getServerAddress(Integer partitionKey) throws PortunusException;
}
