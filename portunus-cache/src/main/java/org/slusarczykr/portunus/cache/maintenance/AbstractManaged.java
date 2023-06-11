package org.slusarczykr.portunus.cache.maintenance;

public abstract class AbstractManaged implements Managed {

    private final ManagedService managedService;

    protected AbstractManaged(ManagedService managedService) {
        this.managedService = managedService;
        managedService.add(this);
    }

    public ManagedService getManagedService() {
        return managedService;
    }
}
