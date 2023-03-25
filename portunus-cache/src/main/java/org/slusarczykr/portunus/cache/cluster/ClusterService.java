package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.ClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.ClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;

public interface ClusterService extends Service {

    DiscoveryService getDiscoveryService();

    PartitionService getPartitionService();

    ClusterConfigService getClusterConfigService();

    ConversionService getConversionService();

    ClusterEventPublisher getClusterEventPublisher();

    ClusterEventConsumer getClusterEventConsumer();
}
