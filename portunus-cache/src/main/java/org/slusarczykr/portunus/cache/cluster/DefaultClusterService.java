package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.ClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.ClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.cluster.service.ServiceManager;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

public class DefaultClusterService extends AbstractManaged implements ClusterService {

    private ClusterConfig clusterConfig;
    private final ServiceManager serviceManager;

    public static DefaultClusterService newInstance(ClusterConfig clusterConfig) {
        return new DefaultClusterService(clusterConfig);
    }

    private DefaultClusterService(ClusterConfig clusterConfig) {
        try {
            this.serviceManager = DefaultServiceManager.newInstance(this);
            this.clusterConfig = clusterConfig;
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus cluster could not be initialized", e);
        }
    }

    @Override
    public boolean isInitialized() {
        return serviceManager.isInitialized();
    }

    @Override
    public void initialize() throws PortunusException {
        serviceManager.loadServices();
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    @Override
    public void overrideClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
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
        T service = serviceManager.getService(clazz);

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

    @Override
    public void shutdown() {

    }
}
