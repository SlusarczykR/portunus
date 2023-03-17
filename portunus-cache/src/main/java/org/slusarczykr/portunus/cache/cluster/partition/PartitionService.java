package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface PartitionService {

    String getPartitionOwner(String key) throws PortunusException;

    void register(String address) throws PortunusException;

    void unregister(String address) throws PortunusException;
}
