package org.slusarczykr.portunus.cache.cluster.server.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.DistributedCache;
import org.slusarczykr.portunus.cache.DistributedCache.OperationType;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.*;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.*;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.api.test.PortunusTestApiProtos;
import org.slusarczykr.portunus.cache.api.test.PortunusTestApiProtos.GetStringEntryDocument;
import org.slusarczykr.portunus.cache.api.test.PortunusTestApiProtos.GetStringEntryQuery;
import org.slusarczykr.portunus.cache.api.test.PortunusTestApiProtos.StringCacheEntryDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.PortunusClusterInstance;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.SyncServerStateRequest;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PortunusGRPCService extends PortunusServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(PortunusGRPCService.class);

    private final ClusterService clusterService;

    public PortunusGRPCService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public void anyEntry(ContainsAnyEntryQuery request, StreamObserver<ContainsAnyEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.IS_EMPTY, () -> createContainsAnyEntryDocument(request));
    }

    private ContainsAnyEntryDocument createContainsAnyEntryDocument(ContainsAnyEntryQuery query) {
        return ContainsAnyEntryDocument.newBuilder()
                .setAnyEntry(anyEntry(query))
                .build();
    }

    private boolean anyEntry(ContainsAnyEntryQuery query) {
        return Optional.ofNullable(getDistributedCache(query.getCacheName()))
                .map(DistributedCache::anyLocalEntry)
                .orElse(false);
    }

    @Override
    public void containsEntry(ContainsEntryQuery request, StreamObserver<ContainsEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.CONTAINS_KEY, () -> createContainsEntryDocument(request));
    }

    @SneakyThrows
    private ContainsEntryDocument createContainsEntryDocument(ContainsEntryQuery query) {
        Distributed<?> distributed = getDistributedKey(query);
        boolean containsEntry = containsEntry(query, distributed);
        return ContainsEntryDocument.newBuilder()
                .setContainsEntry(containsEntry)
                .build();
    }

    private boolean containsEntry(ContainsEntryQuery query, Distributed<?> distributed) {
        return Optional.ofNullable(getDistributedCache(query.getCacheName()))
                .map(it -> containsEntry(it, distributed.get()))
                .orElse(false);
    }

    @SneakyThrows
    private <K> boolean containsEntry(Cache<K, ?> cache, K key) {
        return cache.containsKey(key);
    }

    private <T extends Serializable> Distributed<T> getDistributedKey(ContainsEntryQuery query) throws PortunusException {
        try {
            return toDistributed(query.getEntryKey());
        } catch (Exception e) {
            throw new PortunusException("Unable to deserialize request");
        }
    }

    private <K extends Serializable> Distributed<K> toDistributed(ByteString bytes) {
        return DistributedWrapper.fromBytes(bytes.toByteArray());
    }

    @Override
    public void getCache(GetCacheCommand request, StreamObserver<GetCacheDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ALL_ENTRIES, () -> getCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetCacheDocument getCacheEntries(GetCacheCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getName());
        List<CacheEntryDTO> cacheEntries = cache.allEntries().stream()
                .map(PortunusGRPCService::toCacheEntry)
                .toList();

        return GetCacheDocument.newBuilder()
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    private static <K extends Serializable, V extends Serializable> CacheEntryDTO toCacheEntry(Cache.Entry<K, V> entry) {
        return CacheEntryDTO.newBuilder()
                .setKey(DistributedWrapper.from((entry.getKey())).getByteString())
                .setValue(DistributedWrapper.from((entry.getValue())).getByteString())
                .build();
    }

    @Override
    public void getCacheEntry(GetEntryCommand request, StreamObserver<GetEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ENTRY, () -> getCacheEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetEntryDocument getCacheEntry(GetEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Distributed<K> distributed = toDistributed(command.getKey());
        CacheEntryDTO cacheEntry = cache.getEntry(distributed.get())
                .map(it -> clusterService.getConversionService().convert(it))
                .orElse(null);

        return GetEntryDocument.newBuilder()
                .setCacheEntry(cacheEntry)
                .build();
    }

    @Override
    public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ALL_ENTRIES, this::getLocalPartitions);
    }

    private GetPartitionsDocument getLocalPartitions() {
        List<PartitionDTO> partitions = clusterService.getPartitionService().getPartitions().stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();

        return GetPartitionsDocument.newBuilder()
                .addAllPartitions(partitions)
                .build();
    }

    @Override
    public void putEntry(PutEntryCommand request, StreamObserver<PutEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.PUT, () -> putEntry(request));
    }

    private <K extends Serializable, V extends Serializable> PutEntryDocument putEntry(PutEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Cache.Entry<K, V> entry = clusterService.getConversionService().convert(command.getCacheEntry());
        cache.put(entry);

        return PutEntryDocument.newBuilder()
                .setStatus(true)
                .build();
    }

    @Override
    public void removeEntry(RemoveEntryCommand request, StreamObserver<RemoveEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REMOVE, () -> removeEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> RemoveEntryDocument removeEntry(RemoveEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Distributed<K> distributed = toDistributed(command.getKey());

        return cache.getEntry(distributed.get())
                .map(it -> {
                    removeEntry(cache, it.getKey());
                    return RemoveEntryDocument.newBuilder()
                            .setCacheEntry(clusterService.getConversionService().convert(it))
                            .build();

                })
                .orElseThrow(() -> new PortunusException("Could not remove entry"));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> Cache.Entry<K, V> removeEntry(Cache<K, V> cache, K key) {
        return cache.remove(key);
    }

    @Override
    public void sendClusterEvent(ClusterEvent request, StreamObserver<Empty> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.SEND_CLUSTER_EVENT, () -> handleEvent(request));
    }

    private Empty handleEvent(ClusterEvent event) {
        clusterService.getClusterEventConsumer().consumeEvent(event);
        return Empty.getDefaultInstance();
    }

    @Override
    public void sendPartitionEvent(PartitionEvent request, StreamObserver<Empty> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.SEND_PARTITION_EVENT, () -> handleEvent(request));
    }

    private Empty handleEvent(PartitionEvent event) {
        clusterService.getClusterEventConsumer().consumeEvent(event);
        return Empty.getDefaultInstance();
    }

    @Override
    public void sendRequestVote(AppendEntry request, StreamObserver<RequestVoteResponse> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.VOTE, () -> {
            log.debug("Received request vote from server with id: {}", request.getServerId());
            clusterService.getLeaderElectionStarter().stopLeaderScheduledJobsOrReset();
            RequestVote.Response requestVoteResponse = voteForLeader(request);

            return clusterService.getConversionService().convert(requestVoteResponse);
        });
    }

    private RequestVote.Response voteForLeader(AppendEntry request) {
        RequestVote requestVote = clusterService.getConversionService().convert(request);
        return clusterService.getRequestVoteService().vote(requestVote);
    }

    private RequestVoteResponse startLeaderCandidacy() throws PaxosLeaderElectionException {
        boolean leader = clusterService.getLeaderElectionService().startLeaderCandidacy();
        return createRequestVoteResponse(leader);
    }

    private RequestVoteResponse createRequestVoteResponse(boolean leader) {
        PaxosServer paxosServer = getPaxosServer();

        return RequestVoteResponse.newBuilder()
                .setAccepted(leader)
                .setServerId(paxosServer.getIdValue())
                .setTerm(paxosServer.getTermValue())
                .build();
    }

    private PaxosServer getPaxosServer() {
        PortunusClusterInstance portunusClusterInstance = clusterService.getPortunusClusterInstance();
        return portunusClusterInstance.localMember().getPaxosServer();
    }

    @Override
    public void sendHeartbeats(AppendEntry request, StreamObserver<AppendEntryResponse> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.SYNC_STATE, () -> {
            log.trace("Received heartbeat from leader with id: {}", request.getServerId());
            boolean conflict = false;

            if (clusterService.getLeaderElectionStarter().stopLeaderScheduledJobsOrReset()) {
                log.error("Heartbeat message received while the current server is already the leader!");
                conflict = true;
            }
            return AppendEntryResponse.newBuilder()
                    .setServerId(getPaxosServer().getIdValue())
                    .setConflict(conflict)
                    .build();
        }, true, true);
    }

    @Override
    public void syncServerState(SyncServerStateRequest request, StreamObserver<AppendEntryResponse> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.SYNC_STATE, () -> {
            log.trace("Received sync server state from leader with id: {}", request.getServerId());
            syncServerState(request.getVirtualPortunusNodeList(), request.getPartitionList());

            return AppendEntryResponse.newBuilder()
                    .setServerId(getPaxosServer().getIdValue())
                    .setConflict(false)
                    .build();
        }, true, true);
    }

    private void syncServerState(List<VirtualPortunusNodeDTO> virtualPortunusNodes,
                                 List<PartitionDTO> partitions) {
        SortedMap<String, VirtualPortunusNode> virtualPortunusNodeMap = toVirtualPortunusNodeMap(virtualPortunusNodes);
        updateServerDiscovery(virtualPortunusNodeMap);
        updatePartitions(partitions, virtualPortunusNodeMap);
    }

    private void updateServerDiscovery(SortedMap<String, VirtualPortunusNode> virtualPortunusNodeMap) {
        Set<Address> portunusNodes = getPhysicalNodeAddresses(virtualPortunusNodeMap);
        clusterService.getDiscoveryService().registerRemoteServers(portunusNodes);
    }

    private void updatePartitions(List<PartitionDTO> partitions, SortedMap<String, VirtualPortunusNode> virtualPortunusNodeMap) {
        Map<Integer, Partition> partitionMap = toPartitionsMap(partitions);
        clusterService.getPartitionService().update(virtualPortunusNodeMap, partitionMap);
    }

    private Map<Integer, Partition> toPartitionsMap(List<PartitionDTO> partitions) {
        return partitions.stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .collect(Collectors.toMap(Partition::getPartitionId, it -> it));
    }

    private SortedMap<String, VirtualPortunusNode> toVirtualPortunusNodeMap(List<VirtualPortunusNodeDTO> virtualPortunusNodes) {
        return virtualPortunusNodes.stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, ConcurrentSkipListMap::new));
    }

    private static Set<Address> getPhysicalNodeAddresses(SortedMap<String, VirtualPortunusNode> virtualPortunusNodeMap) {
        return virtualPortunusNodeMap.values().stream()
                .map(VirtualPortunusNode::getPhysicalNodeKey)
                .map(Address::from)
                .collect(Collectors.toSet());
    }

    @Override
    public void replicate(ReplicatePartitionCommand request, StreamObserver<ReplicatePartitionDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REPLICATE_PARTITION, () -> {
            CacheChunk cacheChunk = clusterService.getConversionService().convert(request.getCacheChunk());
            log.debug("Received replicate partition [{}] command", cacheChunk.getPartitionId());
            clusterService.getReplicaService().registerPartitionReplica(cacheChunk);

            return ReplicatePartitionDocument.newBuilder()
                    .setStatus(true)
                    .build();
        });
    }

    @Override
    public void migrate(MigratePartitionsCommand request, StreamObserver<MigratePartitionsDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.MIGRATE_PARTITIONS, () -> {
            List<CacheChunk> cacheChunks = convert(request.getCacheChunksList());

            clusterService.getMigrationService().migrateToLocalServer(cacheChunks, request.getReplicate());

            return MigratePartitionsDocument.newBuilder()
                    .setStatus(true)
                    .build();
        }, false, false);
    }

    @Override
    public void register(RegisterMemberCommand request, StreamObserver<RegisterMemberDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REGISTER_MEMBER, () ->
                RegisterMemberDocument.newBuilder()
                        .setStatus(true)
                        .build());
    }

    @Override
    public void getStringCacheEntry(GetStringEntryQuery request, StreamObserver<GetStringEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ENTRY, () -> getStringCacheEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetStringEntryDocument getStringCacheEntry(GetStringEntryQuery command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        StringCacheEntryDTO cacheEntry = cache.getEntry((K) command.getKey())
                .map(it -> StringCacheEntryDTO.newBuilder()
                        .setKey((String) it.getKey())
                        .setValue((String) it.getValue())
                        .build())
                .orElse(null);

        return GetStringEntryDocument.newBuilder()
                .setCacheEntry(cacheEntry)
                .build();
    }

    @Override
    public void putStringEntry(PortunusTestApiProtos.PutStringEntryCommand request, StreamObserver<PutEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.PUT, () -> putStringEntry(request));
    }

    private <K extends Serializable, V extends Serializable> PutEntryDocument putStringEntry(PortunusTestApiProtos.PutStringEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        StringCacheEntryDTO cacheEntryDTO = command.getCacheEntry();
        Cache.Entry<K, V> entry = (Cache.Entry<K, V>) new DefaultCache.Entry<>(cacheEntryDTO.getKey(), cacheEntryDTO.getValue());
        cache.put(entry);

        return PutEntryDocument.newBuilder()
                .setStatus(true)
                .build();
    }

    private List<CacheChunk> convert(List<PortunusApiProtos.CacheChunkDTO> cacheChunksList) {
        return cacheChunksList.stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();
    }

    private <K extends Serializable, V extends Serializable> DistributedCache<K, V> getDistributedCache(String name) {
        log.trace("Getting distributed cache: '{}'", name);
        return (DistributedCache<K, V>) clusterService.getPortunusClusterInstance().getCache(name);
    }

    private <T> void completeWith(AddressDTO fromDTO, StreamObserver<T> responseObserver, OperationType operationType,
                                  Supplier<T> onNext) {
        completeWith(fromDTO, responseObserver, operationType, onNext, true, false);
    }

    private <T> void completeWith(AddressDTO fromDTO, StreamObserver<T> responseObserver, OperationType operationType,
                                  Supplier<T> onNext, boolean registerRemoteServer, boolean trace) {
        Address from = clusterService.getConversionService().convert(fromDTO);
        logRequest(operationType, from, trace);

        if (registerRemoteServer) {
            registerRemoteServerIfAbsent(from);
        }
        responseObserver.onNext(onNext.get());
        responseObserver.onCompleted();
    }

    private static void logRequest(OperationType operationType, Address from, boolean trace) {
        String logEntry = String.format("Received '%s' command from '%s'", operationType, from);

        if (trace) {
            log.trace(logEntry);
        } else {
            log.debug(logEntry);
        }
    }

    private void registerRemoteServerIfAbsent(Address address) {
        clusterService.getDiscoveryService().registerRemoteServer(address);
    }
}
