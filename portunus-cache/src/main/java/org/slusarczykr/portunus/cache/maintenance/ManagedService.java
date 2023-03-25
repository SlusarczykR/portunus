package org.slusarczykr.portunus.cache.maintenance;

import java.util.List;

public interface ManagedService {

    void add(Managed managed);

    List<Managed> getAllManaged();

    void shutdownAll();
}
