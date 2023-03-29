package org.slusarczykr.portunus.cache.cluster.server.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetCacheDocument;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsAnyEntryQuery;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.manager.CacheManager;
import org.slusarczykr.portunus.cache.manager.DefaultCacheManager;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class PortunusGRPCService extends PortunusServiceImplBase {

    private final ClusterService clusterService;
    private final CacheManager cacheManager;

    public PortunusGRPCService() {
        this.clusterService = DefaultClusterService.getInstance();
        this.cacheManager = DefaultCacheManager.getInstance();
    }

    @Override
    public void anyEntry(ContainsAnyEntryQuery request, StreamObserver<ContainsAnyEntryDocument> responseObserver) {
        responseObserver.onNext(createContainsAnyEntryDocument(request));
        responseObserver.onCompleted();
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
        responseObserver.onNext(createContainsEntryDocument(request));
        responseObserver.onCompleted();
    }

    @SneakyThrows
    private ContainsEntryDocument createContainsEntryDocument(ContainsEntryQuery query) {
        Distributed<?> distributed = wrap(query);
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

    private <T extends Serializable> Distributed<T> wrap(ContainsEntryQuery query) throws PortunusException {
        try {
            return Distributed.DistributedWrapper.fromBytes(query.getEntryKey().toByteArray());
        } catch (Exception e) {
            throw new PortunusException("Unable to deserialize request");
        }
    }

    @Override
    public void getCache(GetCacheCommand request, StreamObserver<GetCacheDocument> responseObserver) {
        responseObserver.onNext(getLocalPartition(request));
        responseObserver.onCompleted();
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
        responseObserver.onNext(getLocalPartitions());
        responseObserver.onCompleted();
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
    public void sendEvent(ClusterEvent request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(handleEvent(request));
        responseObserver.onCompleted();
    }

    private Empty handleEvent(ClusterEvent event) {
        clusterService.getClusterEventConsumer().consumeEvent(event);
        return Empty.getDefaultInstance();
    }
}
