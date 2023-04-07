package org.slusarczykr.portunus.cache.cluster;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.config.DefaultClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.DefaultConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.DefaultClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.DefaultClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.cluster.service.ServiceManager;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultServiceManager implements ServiceManager {

    private final ClusterService clusterService;

    private final Map<String, Service> services = new ConcurrentHashMap<>();

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReentrantLock lock = new ReentrantLock();

    private DefaultServiceManager(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public static DefaultServiceManager newInstance(ClusterService clusterService) {
        return new DefaultServiceManager(clusterService);
    }

    @Override
    public boolean isInitialized() {
        return initialized.get();
    }

    @Override
    public void loadServices() {
        try {
            lock.lock();

            if (!initialized.get()) {
                initializeServices();
                initialized.set(true);
            }
        } finally {
            lock.unlock();
        }
    }

    private void initializeServices() {
        initializeService(DefaultClusterConfigService.newInstance(clusterService));
        initializeService(DefaultPartitionService.newInstance(clusterService));
        initializeService(DefaultDiscoveryService.newInstance(clusterService));
        initializeService(DefaultConversionService.newInstance(clusterService));
        initializeService(DefaultClusterEventPublisher.newInstance(clusterService));
        initializeService(DefaultClusterEventConsumer.newInstance(clusterService));
    }

    @SneakyThrows
    private void initializeService(Service service) {
        service.initialize();
        services.put(service.getName(), service);
    }

    @Override
    public List<Service> getServices() {
        return services.values().stream()
                .toList();
    }

    @Override
    public <T extends Service> T getService(Class<T> clazz) {
        return Optional.ofNullable((T) services.get(clazz.getSimpleName()))
                .orElseThrow(() -> new InvalidPortunusStateException(String.format("Service '%s' was not found", clazz.getSimpleName())));
    }
}
