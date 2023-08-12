package org.slusarczykr.portunus.cache.maintenance;

import org.slf4j.Logger;

public abstract class AbstractManaged implements Managed {

    private final ManagedService managedService;

    protected AbstractManaged(ManagedService managedService) {
        this.managedService = managedService;
        managedService.add(this);
    }

    protected abstract Logger getLogger();

    public ManagedService getManagedService() {
        return managedService;
    }
}
