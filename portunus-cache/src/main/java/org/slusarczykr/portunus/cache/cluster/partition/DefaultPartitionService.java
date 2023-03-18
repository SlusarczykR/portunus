package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public class DefaultPartitionService implements PartitionService {

    private static final DefaultPartitionService INSTANCE = new DefaultPartitionService();
    private final PortunusConsistentHashingCircle partitionOwnerCircle;

    private DefaultPartitionService() {
        this.partitionOwnerCircle = new PortunusConsistentHashingCircle();
    }

    public static DefaultPartitionService getInstance() {
        return INSTANCE;
    }

    @Override
    public Partition getPartition(String key) {
        return null;
    }

    @Override
    public String getPartitionOwner(String key) throws PortunusException {
        return partitionOwnerCircle.getServerAddress(key);
    }

    @Override
    public void register(Address address) throws PortunusException {
        partitionOwnerCircle.add(address);
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        partitionOwnerCircle.remove(address);
    }
}
