package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.SneakyThrows;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractConcurrentService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultPartitionService extends AbstractConcurrentService implements PartitionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionService.class);

    private static final int DEFAULT_NUMBER_OF_PARTITIONS = 157;

    private final PortunusConsistentHashingCircle partitionOwnerCircle;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    private DefaultPartitionService(ClusterService clusterService) {
        super(clusterService);
        this.partitionOwnerCircle = new PortunusConsistentHashingCircle();
    }

    public static DefaultPartitionService newInstance(ClusterService clusterService) {
        return new DefaultPartitionService(clusterService);
    }

    @Override
    public SortedMap<String, VirtualPortunusNode> getPartitionOwnerCircle() {
        return partitionOwnerCircle.get();
    }

    @Override
    public boolean isLocalPartition(Object key) throws PortunusException {
        int partitionId = getPartitionId(key);
        String partitionOwnerAddress = getOwnerAddress(partitionId).toPlainAddress();
        String localServerAddress = clusterService.getClusterConfigService().getLocalServerPlainAddress();

        return localServerAddress.equals(partitionOwnerAddress);
    }

    @Override
    public int getPartitionId(Object key) {
        return generateHashCode(key) % DEFAULT_NUMBER_OF_PARTITIONS;
    }

    @Override
    public Partition register(Partition partition) {
        return withWriteLock(() -> {
            log.info("Registering partition: {}", partition);
            return partitions.put(partition.getPartitionId(), partition);
        });
    }

    @Override
    public Partition getPartition(int partitionId) {
        return withWriteLock(() -> getOrCreate(partitionId));
    }

    @Override
    public Partition getLocalPartition(int partitionId) throws PortunusException {
        Partition partition = withReadLock(() -> partitions.get(partitionId));

        return Optional.ofNullable(partition)
                .orElseThrow(() -> new PortunusException(String.format("Partition '%s' does not exists", partitionId)));
    }

    @Override
    public Map<Integer, Partition> getPartitionMap() {
        return withReadLock(() -> new HashMap<>(partitions));
    }

    private int generateHashCode(Object key) {
        return new HashCodeBuilder(17, 37)
                .append(key)
                .toHashCode();
    }

    @Override
    public Partition getPartitionForKey(Object key) {
        log.info("Getting partition for key: {}", key);
        int partitionId = getPartitionId(key);
        return withWriteLock(() -> getOrCreate(partitionId));
    }

    private Partition getOrCreate(int partitionId) {
        if (partitions.containsKey(partitionId)) {
            log.info("Getting partition with id: {}", partitionId);
            return partitions.get(partitionId);
        }
        return createPartition(partitionId);
    }

    private Partition createPartition(int partitionId) {
        log.info("Creating partition for id: {}", partitionId);
        Partition partition = newPartition(partitionId);
        partitions.put(partitionId, partition);
        log.info("Created partition: {}", partition);

        if (partition.isLocal()) {
            clusterService.getReplicaService().replicatePartition(partition);
        }
        return partition;
    }

    @Override
    public List<Partition> getLocalPartitions() {
        return withReadLock(() -> {
            return new ArrayList<>(partitions.values());
        });
    }

    @SneakyThrows
    private Partition newPartition(int partitionId) {
        Address serverAddress = getOwnerAddress(partitionId);
        PortunusServer server = clusterService.getDiscoveryService().getServerOrThrow(serverAddress);
        log.info("Server for address: {}, server: {}", serverAddress, server);

        return new Partition(partitionId, server);
    }

    @Override
    public Address getPartitionOwner(String key) throws PortunusException {
        int partitionId = getPartitionId(key);
        return getOwnerAddress(partitionId);
    }

    @SneakyThrows
    private Address getOwnerAddress(int partitionId) {
        log.info("Partition owner circle servers: {}", Arrays.toString(partitionOwnerCircle.getAddresses().toArray()));
        return Address.from(partitionOwnerCircle.getServerAddress(partitionId));
    }

    @Override
    public void register(PortunusServer server) throws PortunusException {
        withWriteLock(() -> registerAddress(server.getAddress()));
        executePartitionsRebalance();
    }

    @SneakyThrows
    private void registerAddress(Address address) {
        log.info("Registering '{}' server", address);
        partitionOwnerCircle.add(address);
    }

    private void executePartitionsRebalance() {
        CompletableFuture.runAsync(() -> {
            log.info("Start executing partitions rebalance procedure");
            withReadLock(this::rebalance);
            log.info("Partitions rebalance has been finished");
        });
    }

    private void rebalance() {
        Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();
        Map<Address, List<Partition>> partitionsByOwner = getOwnerPartitions(localServerAddress).stream()
                .collect(Collectors.groupingBy(it -> getOwnerAddress(it.getPartitionId())));
        partitionsByOwner.remove(localServerAddress);

        partitionsByOwner.forEach(this::migrate);
    }

    private void migrate(Address address, List<Partition> partitions) {
        clusterService.getMigrationService().migrate(partitions, address);
    }

    private List<Partition> getOwnerPartitions(Address address) {
        return partitions.values().stream()
                .filter(partition -> partition.getOwnerAddress().equals(address))
                .toList();
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> unregisterAddress(address));
    }

    @SneakyThrows
    private void unregisterAddress(Address address) {
        List<Integer> ownerPartitionsIds = getOwnerPartitionsIds(address);
        ownerPartitionsIds.forEach(partitions.keySet()::remove);
        partitionOwnerCircle.remove(address);
    }

    private List<Integer> getOwnerPartitionsIds(Address address) {
        return partitions.entrySet().stream()
                .filter(it -> it.getValue().getOwnerAddress().equals(address))
                .map(Map.Entry::getKey)
                .toList();
    }

    @Override
    public List<String> getRegisteredAddresses() {
        return withReadLock(() -> {
            return new ArrayList<>(partitionOwnerCircle.getAddresses());
        });
    }

    @Override
    public void update(SortedMap<String, VirtualPortunusNode> virtualPortunusNodes,
                       Map<Integer, Partition> partitions) {
        withWriteLock(() -> {
            partitionOwnerCircle.update(virtualPortunusNodes);
            update(partitions);
        });
    }

    @Override
    public Map<PortunusServer, Long> getOwnerPartitionsCount() {
        return withReadLock(() -> {
            Map<PortunusServer, Long> ownerToPartitionCount = partitions.values().stream()
                    .collect(Collectors.groupingBy(Partition::getOwner, Collectors.counting()));

            return clusterService.getDiscoveryService().allServers().stream()
                    .collect(Collectors.toMap(it -> it, it -> getOwnerPartitionCount(ownerToPartitionCount, it)));
        });
    }

    private static Long getOwnerPartitionCount(Map<PortunusServer, Long> ownerToPartitionCount, PortunusServer owner) {
        return Optional.ofNullable(ownerToPartitionCount.get(owner))
                .orElse(0L);
    }

    private void update(Map<Integer, Partition> partitions) {
        log.info("Updating partition map. Current partitions: {}", this.partitions);
        log.info("New partitions: {}", partitions);
        this.partitions.putAll(partitions);
        log.info("Partition map was updated. Partitions: {}", this.partitions);
    }

    @Override
    public String getName() {
        return PartitionService.class.getSimpleName();
    }
}
