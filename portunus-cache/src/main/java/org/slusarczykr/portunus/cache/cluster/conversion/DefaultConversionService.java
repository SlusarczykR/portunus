package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntry;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;

import java.io.Serializable;

public class DefaultConversionService implements ConversionService {

    private static final DefaultConversionService INSTANCE = new DefaultConversionService();

    private final ClusterService clusterService;

    private DefaultConversionService() {
        this.clusterService = DefaultClusterService.getInstance();
    }

    public static DefaultConversionService getInstance() {
        return INSTANCE;
    }

    @Override
    public Partition convert(PortunusApiProtos.Partition partition) {
        return clusterService.getPartitionService().getPartition((int) partition.getKey());
    }

    @Override
    public PortunusApiProtos.Partition convert(Partition partition) {
        return PortunusApiProtos.Partition.newBuilder()
                .setKey(partition.partitionId())
                .build();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntry cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.fromBytes(cacheEntry.getKey().toByteArray());
        Distributed<V> value = Distributed.DistributedWrapper.fromBytes(cacheEntry.getValue().toByteArray());

        return new DefaultCache.Entry<>(key.get(), value.get());
    }

    @Override
    public <K extends Serializable, V extends Serializable> CacheEntry convert(Cache.Entry<K, V> cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.from(cacheEntry.getKey());
        Distributed<V> value = Distributed.DistributedWrapper.from(cacheEntry.getValue());

        return CacheEntry.newBuilder()
                .setKey(key.getByteString())
                .setValue(value.getByteString())
                .build();
    }

    @Override
    public String getName() {
        return ConversionService.class.getSimpleName();
    }
}
