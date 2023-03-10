package org.slusarczykr.portunus.cache.maintenance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

public class DefaultManagedCollector implements ManagedCollector {

    private static final DefaultManagedCollector INSTANCE = new DefaultManagedCollector();

    private final ServiceLoader<Managed> managedServiceLoader;

    public DefaultManagedCollector() {
        this.managedServiceLoader = ServiceLoader.load(Managed.class);
    }

    public static DefaultManagedCollector getInstance() {
        return INSTANCE;
    }

    @Override
    public Collection<Managed> getAllManagedObjects() {
        List<Managed> managedObjects = new ArrayList<>();
        managedServiceLoader.reload();
        managedServiceLoader.iterator().forEachRemaining(managedObjects::add);

        return managedObjects;
    }
}
