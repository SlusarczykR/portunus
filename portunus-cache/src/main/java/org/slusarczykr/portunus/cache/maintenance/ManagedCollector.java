package org.slusarczykr.portunus.cache.maintenance;

import java.util.Collection;

public interface ManagedCollector {

    Collection<Managed> getAllManagedObjects();
}
