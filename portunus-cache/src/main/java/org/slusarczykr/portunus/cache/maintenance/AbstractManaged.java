package org.slusarczykr.portunus.cache.maintenance;

public abstract class AbstractManaged implements Managed {

    protected AbstractManaged() {
        DefaultManagedCollector.getInstance().add(this);
    }
}
