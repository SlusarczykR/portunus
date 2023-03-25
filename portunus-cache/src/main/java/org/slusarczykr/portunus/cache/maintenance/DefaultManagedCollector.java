package org.slusarczykr.portunus.cache.maintenance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultManagedCollector implements ManagedCollector {

    private static final DefaultManagedCollector INSTANCE = new DefaultManagedCollector();

    private static final Set<Managed> allManaged = ConcurrentHashMap.newKeySet();

    private DefaultManagedCollector() {
    }

    public static DefaultManagedCollector getInstance() {
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
}
