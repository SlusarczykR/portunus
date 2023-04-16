package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.SneakyThrows;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultPartitionService extends AbstractService implements PartitionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionService.class);

    private static final int DEFAULT_NUMBER_OF_PARTITIONS = 157;

    private final PortunusConsistentHashingCircle partitionOwnerCircle;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private DefaultPartitionService(ClusterService clusterService) {
        super(clusterService);
        this.partitionOwnerCircle = new PortunusConsistentHashingCircle();
    }

    public static DefaultPartitionService newInstance(ClusterService clusterService) {
        return new DefaultPartitionService(clusterService);
    }

    @Override
    public boolean isLocalPartition(Object key) throws PortunusException {
        int partitionId = getPartitionId(key);
        String partitionOwnerAddress = getServerAddress(partitionId).toPlainAddress();
        String localServerAddress = clusterService.getClusterConfigService().getLocalServerPlainAddress();

        return localServerAddress.equals(partitionOwnerAddress);
    }

    private Address getServerAddress(int partitionId) {
        return withLock(it -> {
            return getOwnerAddress(partitionId);
        }, false);
    }

    @Override
    public int getPartitionId(Object key) {
        return generateHashCode(key) % DEFAULT_NUMBER_OF_PARTITIONS;
    }

    @Override
    public Partition getPartition(int partitionId) {
        return withLock(it -> {
            return it.computeIfAbsent(partitionId, this::createPartition);
        }, true);
    }

    @Override
    public Partition getLocalPartition(int partitionId) throws PortunusException {
        Partition partition = withLock(it -> {
            return partitions.get(partitionId);
        }, false);

        return Optional.ofNullable(partition)
                .orElseThrow(() -> new PortunusException(String.format("Partition '%s' does not exists", partitionId)));
    }

    @Override
    public Map<Integer, Partition> getPartitionMap() {
        return withLock(it -> {
            return new HashMap<>(partitions);
        }, false);
    }

    private int generateHashCode(Object key) {
        return new HashCodeBuilder(17, 37)
                .append(key)
                .toHashCode();
    }

    @Override
    public Partition getPartitionForKey(Object key) {
        int partitionId = getPartitionId(key);

        return withLock(it -> {
            return partitions.computeIfAbsent(partitionId, this::createPartition);
        }, false);
    }

    @Override
    public List<Partition> getLocalPartitions() {
        return withLock(it -> {
            return new ArrayList<>(partitions.values());
        }, false);
    }

    @SneakyThrows
    private Partition createPartition(int partitionId) {
        Address serverAddress = getServerAddress(partitionId);
        PortunusServer server = clusterService.getDiscoveryService().getServerOrThrow(serverAddress);

        return new Partition(partitionId, server);
    }

    @Override
    public Address getPartitionOwner(String key) throws PortunusException {
        int partitionId = getPartitionId(key);
        return getServerAddress(partitionId);
    }

    @SneakyThrows
    private Address getOwnerAddress(int partitionId) {
        return Address.from(partitionOwnerCircle.getServerAddress(partitionId));
    }

    @Override
    public void register(Address address) throws PortunusException {
        withLock((Consumer<Map<Integer, Partition>>) it -> registerAddress(address), true);
    }

    @SneakyThrows
    private void registerAddress(Address address) {
        partitionOwnerCircle.add(address);
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withLock((Consumer<Map<Integer, Partition>>) it -> unregisterAddress(address), true);
    }

    @SneakyThrows
    private void unregisterAddress(Address address) {
        partitionOwnerCircle.remove(address);
    }

    @Override
    public List<String> getRegisteredAddresses() {
        return withLock(it -> {
            return new ArrayList<>(partitionOwnerCircle.getAddresses());
        }, false);
    }

    @Override
    public void updatePartitionMap(Map<Integer, Partition> partitionMap) {
        withLock(this::clearAndUpdatePartitionMap, true);
    }

    private void withLock(Consumer<Map<Integer, Partition>> operation, boolean write) {
        try {
            getLock(write).lock();
            operation.accept(partitions);
        } finally {
            getLock(write).unlock();
        }
    }

    private <T> T withLock(Function<Map<Integer, Partition>, T> operation, boolean write) {
        try {
            getLock(write).lock();
            return operation.apply(partitions);
        } finally {
            getLock(write).unlock();
        }
    }

    private Lock getLock(boolean write) {
        if (write) {
            return lock.writeLock();
        }
        return lock.readLock();
    }

    private void clearAndUpdatePartitionMap(Map<Integer, Partition> partitionMap) {
        log.info("Start updating partition map");
        partitions.clear();
        partitions.putAll(partitionMap);
        log.info("Partition map updated");
    }

    @Override
    public String getName() {
        return PartitionService.class.getSimpleName();
    }
}
