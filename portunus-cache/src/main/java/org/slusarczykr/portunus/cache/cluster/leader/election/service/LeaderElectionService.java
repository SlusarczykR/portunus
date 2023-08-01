package org.slusarczykr.portunus.cache.cluster.leader.election.service;

import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.service.Service;

import java.util.function.Consumer;

public interface LeaderElectionService extends Service {

    boolean startLeaderCandidacy() throws PaxosLeaderElectionException;

    boolean shouldCandidateForLeader();

    void syncServerState(Consumer<Exception> errorHandler);

    void sendHeartbeats(Consumer<Exception> errorHandler);
}
