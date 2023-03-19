package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.SneakyThrows;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.config.DefaultClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPartitionService implements PartitionService {

    private static final int DEFAULT_NUMBER_OF_PARTITIONS = 157;

    private static final DefaultPartitionService INSTANCE = new DefaultPartitionService();

    private final ClusterConfigService clusterConfigService;
    private final DiscoveryService discoveryService;

    private final PortunusConsistentHashingCircle partitionOwnerCircle;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    private DefaultPartitionService() {
        this.clusterConfigService = DefaultClusterConfigService.getInstance();
        this.discoveryService = DefaultDiscoveryService.getInstance();
        this.partitionOwnerCircle = new PortunusConsistentHashingCircle();
    }

    public static DefaultPartitionService getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean isLocalPartition(String key) throws PortunusException {
        int partitionId = getPartitionId(key);
        String partitionOwnerAddress = partitionOwnerCircle.getServerAddress(partitionId);
        String localServerAddress = clusterConfigService.getLocalServerPlainAddress();

        return localServerAddress.equals(partitionOwnerAddress);
    }

    @Override
    public int getPartitionId(String key) {
        return generateHashCode(key) % DEFAULT_NUMBER_OF_PARTITIONS;
    }

    private int generateHashCode(String key) {
        return new HashCodeBuilder(17, 37)
                .append(key)
                .toHashCode();
    }

    @Override
    public Partition getPartition(String key) {
        int partitionId = getPartitionId(key);
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    @SneakyThrows
    private Partition createPartition(int partitionId) {
        String serverAddress = partitionOwnerCircle.getServerAddress(partitionId);
        PortunusServer server = discoveryService.getServerOrThrow(Address.from(serverAddress));

        return new Partition(partitionId, server);
    }

    @Override
    public Address getPartitionOwner(String key) throws PortunusException {
        int partitionId = getPartitionId(key);
        return Address.from(partitionOwnerCircle.getServerAddress(partitionId));
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
