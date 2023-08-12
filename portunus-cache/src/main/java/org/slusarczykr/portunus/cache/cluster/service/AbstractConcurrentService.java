package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractConcurrentService extends AbstractService {

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected AbstractConcurrentService(ClusterService clusterService) {
        super(clusterService);
    }

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
            boolean lockAcquired = getLock(write).tryLock(10, SECONDS);

            if (lockAcquired) {
                operation.run();
            } else {
                throw new OperationFailedException("Could not acquire lock");
            }
        } catch (InterruptedException e) {
            throw new OperationFailedException("Could not acquire lock", e);
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
