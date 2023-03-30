package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractService extends AbstractManaged implements Service {

    private final AtomicBoolean initialized;

    protected AbstractService() {
        super();
        this.initialized = new AtomicBoolean(false);
    }

    @Override
    public void initialize() throws PortunusException {
        onInitialization();
        initialized.set(true);
    }

    @Override
    public boolean isInitialized() {
        return initialized.get();
    }

    protected void onInitialization() throws PortunusException {
    }

    @Override
    public void shutdown() {
    }
}
