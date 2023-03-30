package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.ClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.ClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.cluster.service.ServiceLoader;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;

public class DefaultClusterService extends AbstractService implements ClusterService {

    private static final DefaultClusterService INSTANCE;

    static {
        INSTANCE = new DefaultClusterService();
        ServiceLoader.load();
    }

    private final ServiceLoader serviceLoader;

    public static DefaultClusterService getInstance() {
        return INSTANCE;
    }

    private DefaultClusterService() {
        try {
            this.serviceLoader = DefaultServiceLoader.getInstance();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus cluster could not be initialized", e);
        }
    }

    @Override
    public DiscoveryService getDiscoveryService() {
        return getService(DiscoveryService.class);
    }

    @Override
    public PartitionService getPartitionService() {
        return getService(PartitionService.class);
    }

    @Override
    public ClusterConfigService getClusterConfigService() {
        return getService(ClusterConfigService.class);
    }

    @Override
    public ConversionService getConversionService() {
        return getService(ConversionService.class);
    }

    private <T extends Service> T getService(Class<T> clazz) {
        T service = serviceLoader.getService(clazz);

        if (!service.isInitialized()) {
            throw new InvalidPortunusStateException(String.format("Service '%s' has not been initialized yet", service.getName()));
        }
        return service;
    }

    @Override
    public ClusterEventPublisher getClusterEventPublisher() {
        return getService(ClusterEventPublisher.class);
    }

    @Override
    public ClusterEventConsumer getClusterEventConsumer() {
        return getService(ClusterEventConsumer.class);
    }

    @Override
    public String getName() {
        return ClusterService.class.getSimpleName();
    }
}
