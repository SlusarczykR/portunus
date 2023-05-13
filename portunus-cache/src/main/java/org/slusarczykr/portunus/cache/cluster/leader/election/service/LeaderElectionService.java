package org.slusarczykr.portunus.cache.cluster.leader.election.service;

import org.slusarczykr.portunus.cache.cluster.leader.api.AppendEntry;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.service.Service;

import java.util.function.Consumer;

public interface LeaderElectionService extends Service {

    RequestVote createElectionVote();

    boolean startLeaderCandidacy() throws PaxosLeaderElectionException;

    boolean shouldCandidateForLeader();

    AppendEntry createHeartbeat();

    void syncServerState(Consumer<Exception> errorHandler);

    void sendHeartbeats(Consumer<Exception> errorHandler);
}
