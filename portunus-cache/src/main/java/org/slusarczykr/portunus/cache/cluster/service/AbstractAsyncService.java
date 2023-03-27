package org.slusarczykr.portunus.cache.cluster.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.event.publisher.DefaultClusterEventPublisher;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.util.concurrent.ExecutorService;

public abstract class AbstractAsyncService extends AbstractManaged implements AsyncService {

    private static final Logger log = LoggerFactory.getLogger(AbstractAsyncService.class);

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
