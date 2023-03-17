package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PartitionOwnerCircle {

    void add(String address) throws PortunusException;

    void remove(String address) throws PortunusException;
}
