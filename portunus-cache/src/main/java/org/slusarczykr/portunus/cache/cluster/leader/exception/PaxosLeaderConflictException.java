package org.slusarczykr.portunus.cache.cluster.leader.exception;

public class PaxosLeaderConflictException extends RuntimeException {

    public PaxosLeaderConflictException(String message) {
        super(message);
    }
}
