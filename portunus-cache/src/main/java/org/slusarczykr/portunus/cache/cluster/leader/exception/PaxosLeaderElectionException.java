package org.slusarczykr.portunus.cache.cluster.leader.exception;

public class PaxosLeaderElectionException extends Exception {

    public PaxosLeaderElectionException(String message) {
        super(message);
    }
}
