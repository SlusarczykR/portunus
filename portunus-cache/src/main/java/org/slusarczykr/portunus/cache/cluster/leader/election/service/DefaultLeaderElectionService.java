package org.slusarczykr.portunus.cache.cluster.leader.election.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.api.AppendEntry;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.service.AbstractPaxosService;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DefaultLeaderElectionService extends AbstractPaxosService implements LeaderElectionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    public static DefaultLeaderElectionService newInstance(ClusterService clusterService) {
        return new DefaultLeaderElectionService(clusterService);
    }

    private DefaultLeaderElectionService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    protected void onInitialization() throws PortunusException {
    }

    @Override
    public RequestVote createElectionVote() {
        return new RequestVote(
                paxosServer.getIdValue(),
                paxosServer.getTermValue()
        );
    }

    @Override
    public boolean startLeaderCandidacy() {
        paxosServer.incrementTerm(clusterService.getDiscoveryService().getNumberOfServers());

        if (shouldCandidateForLeader()) {
            return candidateForLeader();
        }
        return false;
    }

    private boolean candidateForLeader() {
        log.info("Starting the candidacy of the server with id {} for the leader...", paxosServer.getIdValue());
        List<RequestVote.Response> responseRequestVotes = sendRequestVoteToFollowers();
        boolean accepted = checkAcceptanceMajority(responseRequestVotes);
        paxosServer.setLeader(accepted);

        if (accepted) {
            log.info("Server with id {} has been accepted by the majority and elected as the leader for the current turn!",
                    paxosServer.getIdValue());
        }
        return accepted;
    }

    private List<RequestVote.Response> sendRequestVoteToFollowers() {
        return clusterService.getDiscoveryService().remoteServers().stream()
                .map(it -> it.sendRequestVote(paxosServer.getIdValue(), paxosServer.getTermValue()))
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();
    }

    private <T extends RequestVote.Response> boolean checkAcceptanceMajority(List<T> responseRequestVotes) {
        Map<Boolean, List<T>> candidatesResponsesByAcceptance = getCandidatesResponsesByAcceptance(responseRequestVotes);
        boolean acceptedByMajority = isAcceptedByMajority(candidatesResponsesByAcceptance);
        log.info(getServerCandidacyVotingStatusMessage(acceptedByMajority));

        return acceptedByMajority;
    }

    private String getServerCandidacyVotingStatusMessage(boolean acceptedByMajority) {
        String acceptanceMessage = acceptedByMajority ? "accepted" : "rejected";
        return String.format("Candidacy of the server with id %d has been %s in the current turn!",
                paxosServer.getIdValue(), acceptanceMessage);
    }

    private <T extends RequestVote.Response> Map<Boolean, List<T>> getCandidatesResponsesByAcceptance(List<T> responseRequestVotes) {
        long currentTerm = paxosServer.getTermValue();

        return responseRequestVotes.stream()
                .map(it -> negateVoteAcceptanceIfCorrupted(it, currentTerm))
                .collect(Collectors.partitioningBy(RequestVote.Response::isAccepted));
    }

    private <T extends RequestVote.Response> T negateVoteAcceptanceIfCorrupted(T responseRequestVote, long currentTerm) {
        if (isVoteCorrupted(responseRequestVote, currentTerm)) {
            log.debug("Vote from server with id '{}' is corrupted. Acceptance value will be corrected", responseRequestVote.getServerId());
            responseRequestVote.setAccepted(!responseRequestVote.isAccepted());
        }
        return responseRequestVote;
    }

    private <T extends RequestVote.Response> boolean isVoteCorrupted(T responseRequestVote, long currentTerm) {
        return responseRequestVote.isAccepted() && responseRequestVote.getTerm() != currentTerm
                || !responseRequestVote.isAccepted() && currentTerm > responseRequestVote.getTerm();
    }

    private <T extends RequestVote.Response> boolean isAcceptedByMajority(Map<Boolean, List<T>> promisesByAcceptance) {
        return countAcceptedRequestVotes(promisesByAcceptance) > countRejectedRequestVotes(promisesByAcceptance, false);
    }

    private <T extends RequestVote.Response> int countRejectedRequestVotes(Map<Boolean, List<T>> promisesByAcceptance, boolean accepted) {
        return promisesByAcceptance.get(accepted).size();
    }

    private <T extends RequestVote.Response> int countAcceptedRequestVotes(Map<Boolean, List<T>> promisesByAcceptance) {
        int candidateRequestVote = 1;
        return countRejectedRequestVotes(promisesByAcceptance, true) + candidateRequestVote;
    }

    @Override
    public boolean shouldCandidateForLeader() {
        if (!clusterService.getDiscoveryService().anyRemoteServerAvailable()) {
            log.debug("No servers available...");
            return true;
        }
        boolean candidateForLeader = calculateCurrentTermModulo() == paxosServer.getIdValue();
        log.info(getShouldCandidateForLeaderMessage(candidateForLeader));

        return candidateForLeader;
    }

    @Override
    public AppendEntry createHeartbeat() {
        return new AppendEntry(paxosServer.getIdValue(), paxosServer.getTermValue());
    }

    @Override
    public void sendHeartbeats(Consumer<Exception> errorHandler) {
        try {
            log.info("Sending heartbeats to followers...");
            clusterService.getDiscoveryService().remoteServers().stream()
                    .map(it -> it.sendPartitionMap(paxosServer.getIdValue(), getPartitions()))
                    .forEach(it -> log.info("Received heartbeat reply from follower with id: {}", it.getServerId()));
        } catch (Exception e) {
            log.error("Error occurred while sending heartbeats to followers!");
            errorHandler.accept(e);
        }
    }

    private List<PartitionDTO> getPartitions() {
        return clusterService.getPartitionService().getPartitionMap().values().stream()
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();
    }

    private String getShouldCandidateForLeaderMessage(boolean candidateForLeader) {
        String ableness = candidateForLeader ? "can" : "cannot";
        return String.format("Server %s candidate for a leader in the current turn...", ableness);
    }

    private long calculateCurrentTermModulo() {
        long serverTerm = paxosServer.getTermValue();
        int numberOfAvailableServers = clusterService.getDiscoveryService().getNumberOfServers();
        log.info("Number of available servers: {}, current server term: {}", numberOfAvailableServers, serverTerm);

        return serverTerm % numberOfAvailableServers;
    }

    @Override
    public String getName() {
        return LeaderElectionService.class.getSimpleName();
    }
}
