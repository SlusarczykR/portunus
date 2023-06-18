package org.slusarczykr.portunus.cache.maintenance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultManagedService implements ManagedService {

    private static final Logger log = LoggerFactory.getLogger(DefaultManagedService.class);

    private final Set<Managed> allManaged = ConcurrentHashMap.newKeySet();

    private DefaultManagedService() {
    }

    public static DefaultManagedService newInstance() {
        return new DefaultManagedService();
    }

    @Override
    public void add(Managed managed) {
        allManaged.add(managed);
    }

    @Override
    public List<Managed> getAllManaged() {
        return new ArrayList<>(allManaged);
    }

    public void shutdownAll() {
        System.out.println("Shutting down managed objects");
        allManaged.forEach(this::shutdownManagedObject);
        System.out.println("Managed objects were shut down");
    }

    private void shutdownManagedObject(Managed managedObject) {
        try {
            managedObject.shutdown();
        } catch (Exception e) {
            log.error("Error occurred during '{}' shutdown", managedObject.getClass().getSimpleName(), e);
        }
    }
}
