package org.slusarczykr.portunus.cache.maintenance;

import java.util.List;

public interface ManagedCollector {

    void add(Managed managed);

    List<Managed> getAllManaged();
}
