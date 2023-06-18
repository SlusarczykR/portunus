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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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
            log.debug("Registering partition: {}", partition);
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
        log.debug("Getting partition for key: {}", key);
        int partitionId = getPartitionId(key);
        return withWriteLock(() -> getOrCreate(partitionId));
    }

    private Partition getOrCreate(int partitionId) {
        if (partitions.containsKey(partitionId)) {
            log.trace("Getting partition with id: {}", partitionId);
            return partitions.get(partitionId);
        }
        return createPartition(partitionId);
    }

    private Partition createPartition(int partitionId) {
        log.debug("Creating partition for id: {}", partitionId);
        Partition partition = newPartition(partitionId);
        partitions.put(partitionId, partition);
        log.debug("Created partition: {}", partition);

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

        return new Partition(partitionId, server);
    }

    @Override
    public Address getPartitionOwner(String key) throws PortunusException {
        int partitionId = getPartitionId(key);
        return getOwnerAddress(partitionId);
    }

    @SneakyThrows
    private Address getOwnerAddress(int partitionId) {
        return Address.from(partitionOwnerCircle.getServerAddress(partitionId));
    }

    @Override
    public void register(PortunusServer server) throws PortunusException {
        withWriteLock(() -> registerAddress(server.getAddress()));
        executePartitionsRebalance(server.getAddress(), true);
    }

    @SneakyThrows
    private void registerAddress(Address address) {
        log.debug("Registering '{}' server", address);
        partitionOwnerCircle.add(address);
    }

    private void executePartitionsRebalance(Address remoteServerAddress, boolean memberJoined) {
        CompletableFuture.runAsync(() -> {
            log.debug("Start executing partitions rebalance procedure");
            rebalance(remoteServerAddress, memberJoined);
            log.debug("Partitions rebalance has been finished");
        });
    }

    private void rebalance(Address remoteServerAddress, boolean memberJoined) {
        Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();

        if (memberJoined) {
            rebalanceOnMemberJoined(remoteServerAddress, localServerAddress);
        } else {
            rebalanceOnMemberLeft(remoteServerAddress, localServerAddress);
        }
    }

    private void rebalanceOnMemberJoined(Address remoteServerAddress, Address localServerAddress) {
        Map<Address, List<Partition>> partitionsByOwner = groupOwnerPartition(localServerAddress, it -> getOwnerAddress(it.getPartitionId()));
        List<Partition> remoteServerPartitions = partitionsByOwner.get(remoteServerAddress);
        Optional.ofNullable(remoteServerPartitions).ifPresent(it -> migrate(remoteServerAddress, it));
    }

    private <T> Map<T, List<Partition>> groupOwnerPartition(Address owner, Function<Partition, T> classifier) {
        return withReadLock(() -> getOwnerPartitions(owner).stream()
                .collect(Collectors.groupingBy(classifier::apply)));
    }

    private void migrate(Address address, List<Partition> partitions) {
        clusterService.getMigrationService().migrate(partitions, address);
        withWriteLock(() -> partitions.forEach(it -> createPartition(it.getPartitionId())));
    }

    private List<Partition> getOwnerPartitions(Address address) {
        return partitions.values().stream()
                .filter(partition -> partition.getOwnerAddress().equals(address))
                .toList();
    }

    private void rebalanceOnMemberLeft(Address remoteServerAddress, Address localServerAddress) {
        Map<Boolean, List<Partition>> partitionsByReplicaOwner =
                groupOwnerPartition(remoteServerAddress, it -> it.isReplicaOwner(localServerAddress));
        List<Partition> partitionsWithReplicaOwners = partitionsByReplicaOwner.get(true);

        migratePartitionReplicas(partitionsWithReplicaOwners, localServerAddress);
    }

    private void migratePartitionReplicas(List<Partition> partitions, Address localServerAddress) {
        if (!partitions.isEmpty()) {
            Map<Address, List<Partition>> partitionsByOwner = partitions.stream()
                    .collect(Collectors.groupingBy(it -> getOwnerAddress(it.getPartitionId())));

            migrateLocalPartitionReplicas(partitionsByOwner, localServerAddress);
            partitionsByOwner.forEach(this::migrate);
        }
    }

    private void migrateLocalPartitionReplicas(Map<Address, List<Partition>> partitionsByOwner, Address localServerAddress) {
        Optional.ofNullable(partitionsByOwner.get(localServerAddress)).ifPresent(it -> {
            partitionsByOwner.remove(localServerAddress);
            clusterService.getMigrationService().migratePartitionReplicas(it);
        });
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> unregisterAddress(address));
        executePartitionsRebalance(address, false);
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
        log.trace("Current partitions: {}", this.partitions);
        log.trace("New partitions: {}", partitions);
        this.partitions.putAll(partitions);
        log.trace("Partitions were updated: {}", this.partitions);
    }

    @Override
    public String getName() {
        return PartitionService.class.getSimpleName();
    }
}
