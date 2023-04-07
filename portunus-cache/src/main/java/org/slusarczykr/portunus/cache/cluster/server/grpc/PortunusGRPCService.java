package org.slusarczykr.portunus.cache.cluster.server.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DistributedCache.OperationType;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.PutEntryCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.PutEntryDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.RemoveEntryCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.RemoveEntryDocument;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryQuery;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.manager.CacheManager;
import org.slusarczykr.portunus.cache.manager.DefaultCacheManager;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class PortunusGRPCService extends PortunusServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RemotePortunusServer.class);

    private final ClusterService clusterService;
    private final CacheManager cacheManager;

    public PortunusGRPCService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.cacheManager = DefaultCacheManager.getInstance();
    }

    @Override
    public void anyEntry(ContainsAnyEntryQuery request, StreamObserver<ContainsAnyEntryDocument> responseObserver) {
        completeWith(responseObserver, OperationType.IS_EMPTY, () -> createContainsAnyEntryDocument(request));
    }

    private ContainsAnyEntryDocument createContainsAnyEntryDocument(ContainsAnyEntryQuery query) {
        return ContainsAnyEntryDocument.newBuilder()
                .setAnyEntry(anyEntry(query))
                .build();
    }

    private boolean anyEntry(ContainsAnyEntryQuery query) {
        return Optional.ofNullable(cacheManager.getCache(query.getCacheName()))
                .map(it -> !it.isEmpty())
                .orElse(false);
    }

    @Override
    public void containsEntry(ContainsEntryQuery request, StreamObserver<ContainsEntryDocument> responseObserver) {
        completeWith(responseObserver, OperationType.CONTAINS_KEY, () -> createContainsEntryDocument(request));
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
        return Optional.ofNullable(cacheManager.getCache(query.getCacheName()))
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
        return Distributed.DistributedWrapper.fromBytes(bytes.toByteArray());
    }

    @Override
    public void getCache(GetCacheCommand request, StreamObserver<GetCacheDocument> responseObserver) {
        completeWith(responseObserver, OperationType.GET_ALL_ENTRIES, () -> getLocalPartition(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> GetCacheDocument getLocalPartition(GetCacheCommand command) {
        Cache<K, V> cache = cacheManager.getCache(command.getName());
        List<CacheEntryDTO> cacheEntries = cache.allEntries().stream()
                .map(PortunusGRPCService::toCacheEntry)
                .toList();

        return GetCacheDocument.newBuilder()
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    private static <K extends Serializable, V extends Serializable> CacheEntryDTO toCacheEntry(Cache.Entry<K, V> entry) {
        return CacheEntryDTO.newBuilder()
                .setKey(Distributed.DistributedWrapper.from((entry.getKey())).getByteString())
                .setValue(Distributed.DistributedWrapper.from((entry.getValue())).getByteString())
                .build();
    }

    @Override
    public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
        completeWith(responseObserver, OperationType.GET_ALL_ENTRIES, this::getLocalPartitions);
    }

    private GetPartitionsDocument getLocalPartitions() {
        List<PartitionDTO> partitions = clusterService.getPartitionService().getLocalPartitions().stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();

        return GetPartitionsDocument.newBuilder()
                .addAllPartitions(partitions)
                .build();
    }

    @Override
    public void putEntry(PutEntryCommand request, StreamObserver<PutEntryDocument> responseObserver) {
        completeWith(responseObserver, OperationType.PUT, () -> putEntry(request));
    }

    private <K extends Serializable, V extends Serializable> PutEntryDocument putEntry(PutEntryCommand command) {
        Cache<K, V> cache = cacheManager.getCache(command.getCacheName());
        Cache.Entry<K, V> entry = clusterService.getConversionService().convert(command.getCacheEntry());
        cache.put(entry);

        return PutEntryDocument.newBuilder()
                .setStatus(true)
                .build();
    }

    @Override
    public void removeEntry(RemoveEntryCommand request, StreamObserver<RemoveEntryDocument> responseObserver) {
        completeWith(responseObserver, OperationType.REMOVE, () -> removeEntry(request));
    }

    @SneakyThrows
    private <K extends Serializable, V extends Serializable> RemoveEntryDocument removeEntry(RemoveEntryCommand command) {
        Cache<K, V> cache = cacheManager.getCache(command.getCacheName());
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
    public void sendEvent(ClusterEvent request, StreamObserver<Empty> responseObserver) {
        completeWith(responseObserver, OperationType.SEND_EVENT, () -> handleEvent(request));
    }

    private Empty handleEvent(ClusterEvent event) {
        clusterService.getClusterEventConsumer().consumeEvent(event);
        return Empty.getDefaultInstance();
    }

    private <T> void completeWith(StreamObserver<T> responseObserver, OperationType operationType, Supplier<T> onNext) {
        log.info("Received '{}' command from remote server", operationType);
        responseObserver.onNext(onNext.get());
        responseObserver.onCompleted();
    }
}
