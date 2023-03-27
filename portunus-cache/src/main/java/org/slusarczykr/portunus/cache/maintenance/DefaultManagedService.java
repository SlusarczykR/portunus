package org.slusarczykr.portunus.cache.maintenance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultManagedService implements ManagedService {

    private static final Logger log = LoggerFactory.getLogger(DefaultManagedService.class);

    private static final DefaultManagedService INSTANCE = new DefaultManagedService();

    private static final Set<Managed> allManaged = ConcurrentHashMap.newKeySet();

    private DefaultManagedService() {
    }

    public static DefaultManagedService getInstance() {
        return INSTANCE;
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
        allManaged.forEach(this::shutdownManagedObject);
    }

    private void shutdownManagedObject(Managed managedObject) {
        try {
            managedObject.shutdown();
        } catch (Exception e) {
            log.error("Error occurred during '{}' shutdown", managedObject.getClass().getSimpleName(), e);
        }
    }
}
