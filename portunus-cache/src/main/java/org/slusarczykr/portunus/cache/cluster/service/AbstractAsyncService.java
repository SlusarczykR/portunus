package org.slusarczykr.portunus.cache.cluster.service;

import java.util.concurrent.ExecutorService;

public abstract class AbstractAsyncService extends AbstractService implements AsyncService {

    private final ExecutorService innerExecutor;

    protected AbstractAsyncService() {
        super();
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
