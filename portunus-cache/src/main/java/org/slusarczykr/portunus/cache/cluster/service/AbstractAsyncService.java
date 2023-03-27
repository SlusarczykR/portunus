package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.util.concurrent.ExecutorService;

public abstract class AbstractAsyncService extends AbstractManaged implements AsyncService {

    private final ExecutorService innerExecutor;

    protected AbstractAsyncService() {
        this.innerExecutor = createExecutorService();
    }

    @Override
    public void execute(Runnable runnable) {
        if (!innerExecutor.isShutdown()) {
            innerExecutor.execute(runnable);
        }
    }

    @Override
    public void shutdown() {
        innerExecutor.shutdown();
    }
}
