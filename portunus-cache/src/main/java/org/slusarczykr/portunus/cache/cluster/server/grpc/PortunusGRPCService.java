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
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.*;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.*;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.*;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.api.test.PortunusTestApiProtos.*;
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
        boolean containsEntry = containsEntry(query);
        return ContainsEntryDocument.newBuilder()
                .setContainsEntry(containsEntry)
                .build();
    }

    private <K extends Serializable, V extends Serializable> boolean containsEntry(ContainsEntryQuery query) {
        DistributedCache<K, V> cache = getDistributedCache(query.getCacheName());
        K key = (K) toDistributed(query.getEntryKey()).get();
        return cache.containsKey(key);
    }

    @Override
    public void getCache(GetCacheCommand request, StreamObserver<GetCacheDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ALL_ENTRIES, () -> getCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetCacheDocument getCacheEntries(GetCacheCommand command) {
        List<CacheEntryDTO> cacheEntries = clusterService.getLocalServer().getCacheEntries(command.getName()).stream()
                .map(this::toCacheEntry)
                .toList();

        return GetCacheDocument.newBuilder()
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    private <K extends Serializable, V extends Serializable> CacheEntryDTO toCacheEntry(Cache.Entry<K, V> entry) {
        return CacheEntryDTO.newBuilder()
                .setKey(DistributedWrapper.from((entry.getKey())).getByteString())
                .setValue(DistributedWrapper.from((entry.getValue())).getByteString())
                .build();
    }

    @Override
    public void getCacheEntries(GetEntriesQuery request, StreamObserver<GetEntriesDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ENTRIES, () -> getCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable> GetEntriesDocument getCacheEntries(GetEntriesQuery query) {
        List<K> keys = fromBytes(query.getKeyList());
        List<CacheEntryDTO> cacheEntries = clusterService.getLocalServer().getCacheEntries(query.getCacheName(), keys).stream()
                .map(this::toCacheEntry)
                .toList();

        return GetEntriesDocument.newBuilder()
                .addAllCacheEntry(cacheEntries)
                .build();
    }

    @Override
    public void getCacheEntriesByPartitionId(GetCacheEntriesByPartitionIdCommand request, StreamObserver<GetCacheEntriesByPartitionIdDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ENTRIES_BY_PARTITION, () -> getCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable> GetCacheEntriesByPartitionIdDocument getCacheEntries(GetCacheEntriesByPartitionIdCommand query) {
        Partition partition = clusterService.getPartitionService().getPartition((int) query.getPartitionId());
        CacheChunk cacheChunk = clusterService.getLocalServer().getCacheChunk(partition);

        return GetCacheEntriesByPartitionIdDocument.newBuilder()
                .setCacheChunk(clusterService.getConversionService().convert(cacheChunk))
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
    public void putEntries(PutEntriesCommand request, StreamObserver<PutEntriesDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REMOVE, () -> putEntries(request));
    }

    private <K extends Serializable, V extends Serializable> PutEntriesDocument putEntries(PutEntriesCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Set<Cache.Entry<K, V>> entries = command.getCacheEntriesList().stream()
                .map(it -> (Cache.Entry<K, V>) clusterService.getConversionService().convert(it))
                .collect(Collectors.toSet());
        cache.putAll(entries);

        return PutEntriesDocument.newBuilder()
                .setStatus(true)
                .build();
    }

    @Override
    public void removeEntries(RemoveEntriesCommand request, StreamObserver<RemoveEntriesDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REMOVE, () -> removeEntries(request));
    }

    private <K extends Serializable, V extends Serializable> RemoveEntriesDocument removeEntries(RemoveEntriesCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Collection<K> keys = command.getKeyList().stream()
                .map(it -> (K) toDistributed(it).get())
                .collect(Collectors.toSet());
        Collection<Cache.Entry<K, V>> removedEntries = cache.removeAll(keys);

        return RemoveEntriesDocument.newBuilder()
                .addAllCacheEntry(toCacheEntriesDTO(removedEntries))
                .build();
    }

    private <K extends Serializable, V extends Serializable> Set<CacheEntryDTO> toCacheEntriesDTO(Collection<Cache.Entry<K, V>> entries) {
        return entries.stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .collect(Collectors.toSet());
    }

    @Override
    public void removeEntry(RemoveEntryCommand request, StreamObserver<RemoveEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REMOVE, () -> removeEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> RemoveEntryDocument removeEntry(RemoveEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        Distributed<K> distributed = toDistributed(command.getKey());

        Optional<CacheEntryDTO> removedEntry = cache.getEntry(distributed.get())
                .map(it -> {
                    Cache.Entry<K, V> entry = removeEntry(cache, it.getKey());
                    return clusterService.getConversionService().convert(entry);
                });

        return RemoveEntryDocument.newBuilder()
                .setCacheEntry(removedEntry.orElse(null))
                .build();
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
    public void containsStringEntry(ContainsStringEntryQuery request, StreamObserver<ContainsEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.CONTAINS_KEY, () -> containsStringEntry(request));
    }

    @SneakyThrows
    private ContainsEntryDocument containsStringEntry(ContainsStringEntryQuery query) {
        boolean containsEntry = containsEntry(query);
        return ContainsEntryDocument.newBuilder()
                .setContainsEntry(containsEntry)
                .build();
    }

    private <K extends Serializable, V extends Serializable> boolean containsEntry(ContainsStringEntryQuery query) {
        DistributedCache<K, V> cache = getDistributedCache(query.getCacheName());
        return cache.containsKey((K) query.getKey());
    }

    @Override
    public void getStringCacheEntry(GetStringEntryQuery request, StreamObserver<GetStringEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ENTRY, () -> getStringCacheEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetStringEntryDocument getStringCacheEntry(GetStringEntryQuery command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        StringCacheEntryDTO cacheEntry = cache.getEntry((K) command.getKey())
                .map(this::toStringCacheEntry)
                .orElse(null);

        return GetStringEntryDocument.newBuilder()
                .setCacheEntry(cacheEntry)
                .build();
    }

    private <K extends Serializable, V extends Serializable> StringCacheEntryDTO toStringCacheEntry(Cache.Entry<K, V> it) {
        return StringCacheEntryDTO.newBuilder()
                .setKey((String) it.getKey())
                .setValue((String) it.getValue())
                .build();
    }

    @Override
    public void getStringCache(GetCacheCommand request, StreamObserver<GetStringCacheDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ALL_ENTRIES, () -> getStringCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetStringCacheDocument getStringCacheEntries(GetCacheCommand command) {
        DistributedCache<K, V> distributedCache = getDistributedCache(command.getName());
        List<StringCacheEntryDTO> cacheEntries = distributedCache.allEntries().stream()
                .map(this::toStringCacheEntry)
                .toList();

        return GetStringCacheDocument.newBuilder()
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    @Override
    public void getStringCacheEntries(GetStringEntriesQuery request, StreamObserver<GetStringCacheDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.GET_ALL_ENTRIES, () -> getStringCacheEntries(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetStringCacheDocument getStringCacheEntries(GetStringEntriesQuery query) {
        DistributedCache<K, V> distributedCache = getDistributedCache(query.getCacheName());
        List<StringCacheEntryDTO> cacheEntries = distributedCache.getEntries((List<K>) query.getKeyList().stream().toList()).stream()
                .map(this::toStringCacheEntry)
                .toList();

        return GetStringCacheDocument.newBuilder()
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    @Override
    public void putStringEntry(PutStringEntryCommand request, StreamObserver<PutEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.PUT, () -> putStringEntry(request));
    }

    private <K extends Serializable, V extends Serializable> PutEntryDocument putStringEntry(PutStringEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());
        StringCacheEntryDTO cacheEntryDTO = command.getCacheEntry();
        Cache.Entry<K, V> entry = (Cache.Entry<K, V>) new DefaultCache.Entry<>(cacheEntryDTO.getKey(), cacheEntryDTO.getValue());
        cache.put(entry);

        return PutEntryDocument.newBuilder()
                .setStatus(true)
                .build();
    }

    @Override
    public void removeStringEntry(RemoveStringEntryCommand request, StreamObserver<RemoveStringEntryDocument> responseObserver) {
        completeWith(request.getFrom(), responseObserver, OperationType.REMOVE, () -> removeEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> RemoveStringEntryDocument removeEntry(RemoveStringEntryCommand command) {
        Cache<K, V> cache = getDistributedCache(command.getCacheName());

        Optional<StringCacheEntryDTO> removedEntry = Optional.ofNullable(removeEntry(cache, (K) command.getKey()))
                .map(this::toStringCacheEntry);

        return RemoveStringEntryDocument.newBuilder()
                .setCacheEntry(removedEntry.orElse(null))
                .build();
    }

    private List<CacheChunk> convert(List<CacheChunkDTO> cacheChunksList) {
        return cacheChunksList.stream()
                .map(it -> clusterService.getConversionService().convert(it, true))
                .toList();
    }

    private <T extends Serializable> List<T> fromBytes(List<ByteString> byteStrings) {
        return byteStrings.stream()
                .map(it -> (T) toDistributed(it).get())
                .toList();
    }

    private <K extends Serializable> Distributed<K> toDistributed(ByteString bytes) {
        return DistributedWrapper.fromBytes(bytes.toByteArray());
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
        if (!clusterService.getPortunusClusterInstance().isShutdown()) {
            Address from = clusterService.getConversionService().convert(fromDTO);
            logRequest(operationType, from, trace);

            if (registerRemoteServer) {
                registerRemoteServerIfAbsent(from);
            }
            responseObserver.onNext(onNext.get());
            responseObserver.onCompleted();
        } else {
            log.error("Server is shutting down. Operation will be cancelled");
        }
    }

    private void logRequest(OperationType operationType, Address from, boolean trace) {
        String logEntry = String.format("Received '%s' command from '%s'", operationType, from);

        if (trace) {
            log.trace(logEntry);
        } else {
            log.debug(logEntry);
        }
    }

    private void registerRemoteServerIfAbsent(Address address) {
        if (!clusterService.getDiscoveryService().isLocalAddress(address)) {
            clusterService.getDiscoveryService().registerRemoteServer(address);
        }
    }
}
