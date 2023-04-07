package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractService extends AbstractManaged implements Service {

    protected final ClusterService clusterService;

    private final AtomicBoolean initialized;

    protected AbstractService(ClusterService clusterService) {
        super();
        this.clusterService = clusterService;
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
