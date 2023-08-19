package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.SneakyThrows;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.partition.replica.ReplicaService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractConcurrentService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
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
        return register(partition, false);
    }

    @Override
    public Partition register(Partition partition, boolean replicate) {
        return withWriteLock(() -> {
            log.debug("Registering partition: {}", partition);
            partitions.put(partition.getPartitionId(), partition);

            if (replicate) {
                replicateIfLocal(partition);
            }
            return partition;
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
        replicateIfLocal(partition);
        return partition;
    }

    private void replicateIfLocal(Partition partition) {
        if (partition.isLocal()) {
            clusterService.getReplicaService().replicatePartition(partition);
        }
    }

    @Override
    public List<Partition> getPartitions() {
        return getPartitions(it -> true, Map.Entry::getValue);
    }

    @Override
    public List<Integer> getPartitionsKeys() {
        return getPartitions(it -> true, Map.Entry::getKey);
    }

    @Override
    public List<Partition> getLocalPartitions() {
        return getPartitions(Partition::isLocal, Map.Entry::getValue);
    }

    @Override
    public List<Integer> getLocalPartitionsKeys() {
        return getPartitions(Partition::isLocal, Map.Entry::getKey);
    }

    private <T> List<T> getPartitions(Predicate<Partition> partitionFilter, Function<Map.Entry<Integer, Partition>, T> partitionEntryMapper) {
        return withReadLock(() ->
                partitions.entrySet().stream()
                        .filter(it -> partitionFilter.test(it.getValue()))
                        .map(partitionEntryMapper)
                        .toList()
        );
    }

    @SneakyThrows
    private Partition newPartition(int partitionId) {
        Address serverAddress = getOwnerAddress(partitionId);
        PortunusServer server = clusterService.getDiscoveryService().getServer(serverAddress, false);

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
        withWriteLock(() -> {
            registerAddress(server.getAddress());
            rebalance(new ArrayList<>(partitions.values()), server.getAddress(), true);
        });
    }

    @SneakyThrows
    private void registerAddress(Address address) {
        log.debug("Registering '{}' server", address);
        partitionOwnerCircle.add(address);
    }

    private void rebalance(Collection<Partition> partitions, Address remoteServerAddress, boolean memberJoined) {
        log.debug("Executing partitions rebalance procedure");
        Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();

        if (memberJoined) {
            CompletableFuture.runAsync(() -> rebalanceOnMemberJoined(partitions, remoteServerAddress, localServerAddress));
        } else {
            rebalanceOnMemberLeft(partitions, remoteServerAddress, localServerAddress);
        }
    }

    private void rebalanceOnMemberJoined(Collection<Partition> partitions, Address remoteServerAddress,
                                         Address localServerAddress) {
        Map<Address, List<Partition>> partitionsByOwner =
                groupOwnerPartition(partitions, localServerAddress, it -> getOwnerAddress(it.getPartitionId()));
        List<Partition> remoteServerPartitions = partitionsByOwner.getOrDefault(remoteServerAddress, List.of());

        if (!remoteServerPartitions.isEmpty()) {
            migrate(remoteServerAddress, remoteServerPartitions, false);
        }
        replicatePartitionsWithoutReplica();
        log.debug("Partitions rebalance after member joined finished");
    }

    private void replicatePartitionsWithoutReplica() {
        withReadLock(() -> {
            List<Partition> localPartitionsWithoutReplica = getLocalPartitionsWithoutReplica();

            if (!localPartitionsWithoutReplica.isEmpty()) {
                log.debug("Replicating {} partitions without replica", localPartitionsWithoutReplica.size());
                localPartitionsWithoutReplica.forEach(it -> clusterService.getReplicaService().replicatePartition(it));
            }
        });
    }

    private List<Partition> getLocalPartitionsWithoutReplica() {
        return getLocalPartitions().stream()
                .filter(it -> !it.hasAnyReplicaOwner())
                .toList();
    }

    private <T> Map<T, List<Partition>> groupOwnerPartition(Collection<Partition> partitions, Address owner,
                                                            Function<Partition, T> classifier) {
        return withReadLock(() -> getOwnerPartitions(partitions, owner).stream()
                .collect(Collectors.groupingBy(classifier)));
    }

    private void migrate(Address address, List<Partition> partitions, boolean replicate) {
        log.debug("Migrating to '{}' partitions: {}", address, partitions);
        withWriteLock(() -> partitions.forEach(it -> createPartition(it.getPartitionId())));
        clusterService.getMigrationService().migrate(partitions, address, replicate);
    }

    private List<Partition> getOwnerPartitions(Collection<Partition> partitions, Address address) {
        return partitions.stream()
                .filter(partition -> partition.getOwnerAddress().equals(address))
                .toList();
    }

    private void rebalanceOnMemberLeft(Collection<Partition> partitions, Address remoteServerAddress,
                                       Address localServerAddress) {
        Map<Boolean, List<Partition>> partitionsByReplicaOwner =
                groupOwnerPartition(partitions, remoteServerAddress, it -> it.isReplicaOwner(localServerAddress));
        List<Partition> partitionsWithReplicaOwners = partitionsByReplicaOwner.getOrDefault(true, new ArrayList<>());

        if (!partitionsWithReplicaOwners.isEmpty()) {
            migratePartitionReplicas(partitionsWithReplicaOwners, localServerAddress);
        }
        removeReplicaOwnerFromLinkedPartitions(remoteServerAddress);
        log.debug("Partitions rebalance after member left finished");
    }

    private void removeReplicaOwnerFromLinkedPartitions(Address remoteServerAddress) {
        withWriteLock(() -> {
            List<Partition> localPartitionsByReplicaOwner = getLocalPartitionsByReplicaOwner(remoteServerAddress);

            if (!localPartitionsByReplicaOwner.isEmpty()) {
                log.debug("Removing '{}' replica owner from {} local partitions",
                        remoteServerAddress, localPartitionsByReplicaOwner.size());
                localPartitionsByReplicaOwner.forEach(it -> it.removeReplicaOwner(remoteServerAddress));
            }
        });
    }

    private List<Partition> getLocalPartitionsByReplicaOwner(Address remoteServerAddress) {
        return getLocalPartitions().stream()
                .filter(it -> it.containsReplicaOwner(remoteServerAddress))
                .toList();
    }

    private void migratePartitionReplicas(List<Partition> partitions, Address localServerAddress) {
        Map<Address, List<Partition>> partitionsByOwner = partitions.stream()
                .collect(Collectors.groupingBy(it -> getOwnerAddress(it.getPartitionId())));

        migrateLocalPartitionReplicas(partitionsByOwner, localServerAddress);
        migrateRemotePartitionReplicas(partitionsByOwner);
    }

    private void migrateLocalPartitionReplicas(Map<Address, List<Partition>> partitionsByOwner, Address localServerAddress) {
        List<Partition> localPartitions = partitionsByOwner.getOrDefault(localServerAddress, List.of());

        if (!localPartitions.isEmpty()) {
            partitionsByOwner.remove(localServerAddress);
            clusterService.getMigrationService().migratePartitionReplicas(localPartitions);
            unregisterPartitionReplicas(localPartitions);
        }
    }

    private void unregisterPartitionReplicas(List<Partition> partitions) {
        ReplicaService replicaService = clusterService.getReplicaService();

        partitions.stream()
                .map(Partition::getPartitionId)
                .forEach(replicaService::unregisterPartitionReplica);
    }

    private void migrateRemotePartitionReplicas(Map<Address, List<Partition>> partitionsByOwner) {
        partitionsByOwner.forEach((owner, ownerPartitions) -> {
            migrate(owner, ownerPartitions, true);
            unregisterPartitionReplicas(ownerPartitions);
        });
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> {
            Collection<Partition> currentPartitions = new ArrayList<>(partitions.values());
            unregisterAddress(address);
            rebalance(currentPartitions, address, false);
        });
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

    private void update(Map<Integer, Partition> partitions) {
        log.trace("Current partitions: {}", this.partitions);
        log.trace("New partitions: {}", partitions);
        this.partitions.putAll(partitions);
        log.trace("Partitions were updated: {}", this.partitions);
    }

    @Override
    public Map<PortunusServer, Long> getOwnerPartitionsCount() {
        return withReadLock(() -> {
            Map<PortunusServer, Long> ownerToPartitionCount = partitions.values().stream()
                    .collect(Collectors.groupingBy(Partition::getOwner, Collectors.counting()));

            return clusterService.getDiscoveryService().allServers(false).stream()
                    .collect(Collectors.toMap(it -> it, it -> getOwnerPartitionCount(ownerToPartitionCount, it)));
        });
    }

    private static Long getOwnerPartitionCount(Map<PortunusServer, Long> ownerToPartitionCount, PortunusServer owner) {
        return Optional.ofNullable(ownerToPartitionCount.get(owner))
                .orElse(0L);
    }

    @Override
    public String getName() {
        return PartitionService.class.getSimpleName();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
