package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.partition.circle.ConsistentHashingPartitionOwnerCircle;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public class DefaultPartitionService implements PartitionService {

    private static final DefaultPartitionService INSTANCE = new DefaultPartitionService();
    private final ConsistentHashingPartitionOwnerCircle partitionOwnerCircle;

    private DefaultPartitionService() {
        this.partitionOwnerCircle = new ConsistentHashingPartitionOwnerCircle();
    }

    public static DefaultPartitionService getInstance() {
        return INSTANCE;
    }

    @Override
    public String getPartitionOwner(String key) throws PortunusException {
        return partitionOwnerCircle.getServerAddress(key);
    }

    @Override
    public void register(String address) throws PortunusException {
        partitionOwnerCircle.add(address);
    }

    @Override
    public void unregister(String address) throws PortunusException {
        partitionOwnerCircle.remove(address);
    }
}
