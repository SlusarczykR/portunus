package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.SneakyThrows;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionCreatedEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
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

import static org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent.PartitionEventType.PartitionCreated;

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
        String partitionOwnerAddress = getServerAddress(partitionId).toPlainAddress();
        String localServerAddress = clusterService.getClusterConfigService().getLocalServerPlainAddress();

        return localServerAddress.equals(partitionOwnerAddress);
    }

    private Address getServerAddress(int partitionId) {
        return withReadLock(() -> getOwnerAddress(partitionId));
    }

    @Override
    public int getPartitionId(Object key) {
        return generateHashCode(key) % DEFAULT_NUMBER_OF_PARTITIONS;
    }

    @Override
    public Partition register(Partition partition) {
        return withWriteLock(() -> {
            log.info("Registering partition: {} for server: '{}'", partition.getPartitionId(), partition.getOwnerPlainAddress());
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
        int partitionId = getPartitionId(key);
        return withWriteLock(() -> getOrCreate(partitionId));
    }

    private Partition getOrCreate(int partitionId) {
        if (!partitions.containsKey(partitionId)) {
            log.info("Creating partition for id: {}", partitionId);
            Partition partition = createPartition(partitionId);
            partitions.put(partitionId, partition);
            replicatePartition(partition);
            return partition;
        }
        return partitions.get(partitionId);
    }

    private void replicatePartition(Partition partition) {
        CompletableFuture.runAsync(() -> clusterService.getReplicaService().replicatePartition(partition));
    }

    @Override
    public List<Partition> getLocalPartitions() {
        return withReadLock(() -> {
            return new ArrayList<>(partitions.values());
        });
    }

    @SneakyThrows
    private Partition createPartition(int partitionId) {
        Address serverAddress = getServerAddress(partitionId);
        PortunusServer server = clusterService.getDiscoveryService().getServerOrThrow(serverAddress);
        Partition partition = new Partition(partitionId, server);
        sendPartitionCreatedEvent(partition);

        return partition;
    }

    private void sendPartitionCreatedEvent(Partition partition) {
        PartitionDTO partitionDTO = clusterService.getConversionService().convert(partition);
        PartitionEvent partitionEvent = PartitionEvent.newBuilder()
                .setEventType(PartitionCreated)
                .setPartitionCreated(PartitionCreatedEvent.newBuilder()
                        .setPartition(partitionDTO)
                        .build()
                )
                .build();

        clusterService.getClusterEventPublisher().publishEvent(partitionEvent);
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
        withWriteLock(() -> registerAddress(address));
    }

    @SneakyThrows
    private void registerAddress(Address address) {
        log.info("Registering '{}' server", address);
        partitionOwnerCircle.add(address);
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> unregisterAddress(address));
    }

    @SneakyThrows
    private void unregisterAddress(Address address) {
        partitionOwnerCircle.remove(address);
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
    public Map<PortunusServer, Long> getPartitionsCount() {
        return partitions.values().stream()
                .collect(Collectors.groupingBy(Partition::getOwner, Collectors.counting()));
    }

    private void update(Map<Integer, Partition> partitions) {
        log.info("Start updating partition map: {}", partitions);
        this.partitions.putAll(partitions);
        log.info("Partition map was updated");
        log.info("Partition map: {}", this.partitions);
    }

    @Override
    public String getName() {
        return PartitionService.class.getSimpleName();
    }
}
