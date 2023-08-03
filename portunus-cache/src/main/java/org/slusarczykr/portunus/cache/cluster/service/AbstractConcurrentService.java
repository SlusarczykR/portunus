package org.slusarczykr.portunus.cache.cluster.service;

import org.slf4j.Logger;
import org.slusarczykr.portunus.cache.cluster.ClusterService;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public abstract class AbstractConcurrentService extends AbstractService {

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected AbstractConcurrentService(ClusterService clusterService) {
        super(clusterService);
    }

    protected abstract Logger getLogger();

    protected <T> T withWriteLock(Supplier<T> operation) {
        return withLock(operation, true);
    }

    protected <T> T withReadLock(Supplier<T> operation) {
        return withLock(operation, false);
    }

    protected void withWriteLock(Runnable operation) {
        withLock(operation, true);
    }

    protected void withReadLock(Runnable operation) {
        withLock(operation, false);
    }

    private <T> T withLock(Supplier<T> operation, boolean write) {
        try {
            getLogger().trace("Acquiring {} lock", getLockName(write));
            getLock(write).lock();
            return operation.get();
        } finally {
            getLogger().trace("Releasing {} lock", getLockName(write));
            getLock(write).unlock();
        }
    }

    private void withLock(Runnable operation, boolean write) {
        try {
            getLock(write).lock();
            operation.run();
        } finally {
            getLock(write).unlock();
        }
    }

    private Lock getLock(boolean write) {
        if (write) {
            return lock.writeLock();
        }
        return lock.readLock();
    }

    private String getLockName(boolean write) {
        return write ? "write" : "read";
    }
}
