package org.slusarczykr.portunus.cache.cluster.leader.vote.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.leader.vote.factory.RequestVoteFactory;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import static org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote.Response.Status.ACCEPTED;
import static org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote.Response.Status.REJECTED;

public class DefaultRequestVoteService extends AbstractService implements RequestVoteService {

    private static final Logger log = LoggerFactory.getLogger(DefaultRequestVoteService.class);

    private PaxosServer paxosServer;
    private RequestVoteFactory requestVoteFactory;

    public static Service newInstance(ClusterService clusterService) {
        return new DefaultRequestVoteService(clusterService);
    }

    private DefaultRequestVoteService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    protected void onInitialization() throws PortunusException {
        this.paxosServer = clusterService.getPortunusClusterInstance().getPaxosServer();
        this.requestVoteFactory = new RequestVoteFactory();
    }

    @Override
    public RequestVote.Response vote(RequestVote requestVote) {
        log.info("Start voting procedure for leader election of candidate server with id {}...", requestVote.getServerId());
        long candidateTerm = requestVote.getTerm();
        boolean accepted = vote(candidateTerm);
        log.info(getServerCandidacyVotingStatusMessage(accepted, requestVote.getServerId()));

        return requestVoteFactory.create(requestVote.getServerId(), paxosServer.getTermValue(), toResponseStatus(accepted));
    }

    private String getServerCandidacyVotingStatusMessage(boolean accepted, long candidateServerId) {
        String acceptanceMessage = accepted ? "accepted" : "rejected";
        return String.format("Candidacy of the server with id %d has been %s!", candidateServerId, acceptanceMessage);
    }

    private RequestVote.Response.Status toResponseStatus(boolean accepted) {
        return accepted ? ACCEPTED : REJECTED;
    }

    private boolean vote(long candidateTerm) {
        paxosServer.demoteLeader();
        boolean accepted = voteForCandidate(candidateTerm);

        if (accepted) {
            setCurrentTerm(candidateTerm);
        }
        return accepted;
    }

    private void setCurrentTerm(long term) {
        paxosServer.updateTerm(term);
    }

    private boolean voteForCandidate(long candidateTerm) {
        long currentTerm = paxosServer.getTermValue();
        log.info("Current term: {}, candidate term: {}", currentTerm, candidateTerm);
        boolean accepted = candidateTerm > currentTerm;

        return accepted;
    }

    @Override
    public String getName() {
        return RequestVoteService.class.getSimpleName();
    }
}
