package org.slusarczykr.portunus.cache.cluster.service;

import java.util.concurrent.ExecutorService;

public interface AsyncService extends Service {
    void execute(Runnable runnable);

    ExecutorService createExecutorService();
}
