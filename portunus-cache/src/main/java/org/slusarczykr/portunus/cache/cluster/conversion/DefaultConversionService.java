package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;

public class DefaultConversionService implements ConversionService {

    private static final DefaultConversionService INSTANCE = new DefaultConversionService();

    private final PartitionService partitionService;
    private final DiscoveryService discoveryService;

    private DefaultConversionService() {
        this.partitionService = DefaultPartitionService.getInstance();
        this.discoveryService = DefaultDiscoveryService.getInstance();
    }

    public static DefaultConversionService getInstance() {
        return INSTANCE;
    }

    @Override
    public Partition convert(PortunusApiProtos.Partition partition) {
        return partitionService.getPartition((int) partition.getKey());
    }

    @Override
    public PortunusApiProtos.Partition convert(Partition partition) {
        return PortunusApiProtos.Partition.newBuilder()
                .setKey(partition.partitionId())
                .build();
    }
}
