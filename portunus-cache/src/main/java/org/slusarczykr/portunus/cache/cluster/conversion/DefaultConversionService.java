package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

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
    public Partition convert(PortunusApiProtos.PartitionDTO partition) {
        return clusterService.getPartitionService().getPartition((int) partition.getKey());
    }

    @Override
    public PortunusApiProtos.PartitionDTO convert(Partition partition) {
        return PortunusApiProtos.PartitionDTO.newBuilder()
                .setKey(partition.partitionId())
                .build();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntryDTO cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.fromBytes(cacheEntry.getKey().toByteArray());
        Distributed<V> value = Distributed.DistributedWrapper.fromBytes(cacheEntry.getValue().toByteArray());

        return new DefaultCache.Entry<>(key.get(), value.get());
    }

    @Override
    public <K extends Serializable, V extends Serializable> CacheEntryDTO convert(Cache.Entry<K, V> cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.from(cacheEntry.getKey());
        Distributed<V> value = Distributed.DistributedWrapper.from(cacheEntry.getValue());

        return CacheEntryDTO.newBuilder()
                .setKey(key.getByteString())
                .setValue(value.getByteString())
                .build();
    }

    @Override
    public Address convert(AddressDTO address) {
        return new Address(address.getHostname(), address.getPort());
    }

    @Override
    public AddressDTO convert(Address address) {
        return AddressDTO.newBuilder()
                .setHostname(address.hostname())
                .setPort(address.port())
                .build();
    }

    @Override
    public String getName() {
        return ConversionService.class.getSimpleName();
    }
}
