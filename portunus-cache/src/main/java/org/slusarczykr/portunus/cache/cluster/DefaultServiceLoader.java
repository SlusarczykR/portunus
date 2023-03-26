package org.slusarczykr.portunus.cache.cluster;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.config.DefaultClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.DefaultConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.DefaultClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.DefaultClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.cluster.service.ServiceLoader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultServiceLoader implements ServiceLoader {

    private static final DefaultServiceLoader INSTANCE = new DefaultServiceLoader();

    private final Map<String, Service> services = new ConcurrentHashMap<>();

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private DefaultServiceLoader() {
    }

    public static DefaultServiceLoader getInstance() {
        return INSTANCE;
    }

    @Override
    public void loadServices() {
        if (!initialized.get()) {
            initializeServices();
            initialized.set(true);
        }
    }

    private void initializeServices() {
        initializeService(DefaultClusterConfigService.getInstance());
        initializeService(DefaultDiscoveryService.getInstance());
        initializeService(DefaultPartitionService.getInstance());
        initializeService(DefaultConversionService.getInstance());
        initializeService(DefaultClusterEventPublisher.getInstance());
        initializeService(DefaultClusterEventConsumer.getInstance());
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
        return (T) services.get(clazz.getSimpleName());
    }
}
