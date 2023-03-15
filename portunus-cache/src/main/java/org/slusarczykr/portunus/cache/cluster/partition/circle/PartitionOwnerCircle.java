package org.slusarczykr.portunus.cache.cluster.partition.circle;

public interface PartitionOwnerCircle {

    void add(String address);

    void remove(String address);
}
