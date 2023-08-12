package org.slusarczykr.portunus.cache.cluster.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.exception.OperationFailedException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractConcurrentService extends AbstractService {

    private static final Logger log = LoggerFactory.getLogger(AbstractConcurrentService.class);

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
            log.trace("Acquiring {} lock ['{}']", getLockName(write), getName());
            boolean lockAcquired = getLock(write).tryLock(10, SECONDS);

            if (lockAcquired) {
                return executeOperation(operation, write);
            } else {
                throw new OperationFailedException(getLockErrorMessage());
            }
        } catch (Exception e) {
            throw new OperationFailedException(getLockErrorMessage(), e);
        }
    }

    private <T> T executeOperation(Supplier<T> operation, boolean write) {
        try {
            return operation.get();
        } finally {
            log.trace("Releasing {} lock ['{}']", getLockName(write), getName());
            getLock(write).unlock();
        }
    }

    private void withLock(Runnable operation, boolean write) {
        try {
            log.trace("Acquiring {} lock ['{}']", getLockName(write), getName());
            boolean lockAcquired = getLock(write).tryLock(10, SECONDS);

            if (lockAcquired) {
                executeOperation(operation, write);
            } else {
                throw new OperationFailedException(getLockErrorMessage());
            }
        } catch (Exception e) {
            throw new OperationFailedException(getLockErrorMessage(), e);
        }
    }

    private void executeOperation(Runnable operation, boolean write) {
        try {
            operation.run();
        } finally {
            log.trace("Releasing {} lock ['{}']", getLockName(write), getName());
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

    private String getLockErrorMessage() {
        return String.format("Could not acquire lock ['%s']", getName());
    }
}
