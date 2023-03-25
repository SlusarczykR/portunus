package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;

public class DefaultClusterService implements ClusterService {

    private static final DefaultClusterService INSTANCE = new DefaultClusterService();

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
        return serviceLoader.getService(clazz);
    }

    @Override
    public String getName() {
        return ClusterService.class.getSimpleName();
    }
}
