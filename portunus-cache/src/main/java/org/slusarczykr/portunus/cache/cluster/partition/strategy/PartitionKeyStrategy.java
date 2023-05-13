package org.slusarczykr.portunus.cache.cluster.partition.strategy;

public interface PartitionKeyStrategy {

    String getServerAddress(Integer partitionKey);
}
