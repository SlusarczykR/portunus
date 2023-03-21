package org.slusarczykr.portunus.cache.cluster.server.grpc;

import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.conversion.DefaultConversionService;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.manager.CacheManager;
import org.slusarczykr.portunus.cache.manager.DefaultCacheManager;

import java.io.Serializable;
import java.util.List;

public class PortunusGRPCService extends PortunusServiceImplBase {

    private final CacheManager cacheManager;
    private final PartitionService partitionService;
    private final ConversionService conversionService;

    public PortunusGRPCService() {
        this.cacheManager = DefaultCacheManager.getInstance();
        this.partitionService = DefaultPartitionService.getInstance();
        this.conversionService = DefaultConversionService.getInstance();
    }

    @Override
    public void containsEntry(ContainsEntryQuery request, StreamObserver<ContainsEntryDocument> responseObserver) {
        responseObserver.onNext(createContainsEntryDocument(request));
        responseObserver.onCompleted();
    }

    @SneakyThrows
    private ContainsEntryDocument createContainsEntryDocument(ContainsEntryQuery query) {
        Distributed<?> distributed = wrap(query);
        boolean containsEntry = cacheManager.getCache(query.getCacheName()).containsKey(distributed.get());
        return ContainsEntryDocument.newBuilder()
                .setContainsEntry(containsEntry)
                .build();
    }

    private <T extends Serializable> Distributed<T> wrap(ContainsEntryQuery query) throws PortunusException {
        try {
            Class<?> clazz = Class.forName(query.getEntryKeyType());
            return Distributed.DistributedWrapper.fromBytes(query.getEntryKey().toByteArray(), (Class<T>) clazz);
        } catch (Exception e) {
            throw new PortunusException("Unable to deserialize request");
        }
    }

    @Override
    public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
        responseObserver.onNext(createGetPartitionsDocument());
        responseObserver.onCompleted();
    }

    private GetPartitionsDocument createGetPartitionsDocument() {
        List<PortunusApiProtos.Partition> partitions = partitionService.getLocalPartitions().stream()
                .map(conversionService::convert)
                .toList();

        return GetPartitionsDocument.newBuilder()
                .addAllPartitions(partitions)
                .build();
    }
}
