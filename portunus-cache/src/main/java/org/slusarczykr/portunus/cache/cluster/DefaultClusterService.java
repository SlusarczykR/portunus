package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultClusterService implements ClusterService {

    private static final DefaultClusterService INSTANCE = new DefaultClusterService();

    private final Map<String, Service> services = new ConcurrentHashMap<>();

    public static DefaultClusterService getInstance() {
        return INSTANCE;
    }

    private DefaultClusterService() {
        try {
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus cluster could not be initialized", e);
        }
    }

    @Override
    public void initialize() throws PortunusException {
        ServiceLoader.load(Service.class).stream()
                .map(ServiceLoader.Provider::get)
                .forEach(it -> services.put(it.getName(), it));
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
        return (T) services.get(clazz.getSimpleName());
    }

    @Override
    public String getName() {
        return ClusterService.class.getSimpleName();
    }
}
