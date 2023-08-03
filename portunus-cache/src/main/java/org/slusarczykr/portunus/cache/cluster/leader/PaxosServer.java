package org.slusarczykr.portunus.cache.cluster.leader;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class PaxosServer {

    private static final Logger log = LoggerFactory.getLogger(PaxosServer.class);

    private final AtomicInteger id = new AtomicInteger(0);
    private final AtomicLong term = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);

    private final AtomicBoolean leader = new AtomicBoolean(false);

    public PaxosServer(int serverPort) {
        updateServerId(serverPort, 1);
        incrementTerm(1);
    }

    public void updateServerId(int serverPort, int numberOfServers) {
        int serverId = calculateServerId(serverPort, numberOfServers);
        id.set(serverId);
        log.debug("Id {} has been assigned to the server", serverId);
    }

    public void incrementTerm(int numberOfServers) {
        long nextTerm = calculateNextTerm(numberOfServers);
        log.debug("New term: {}", nextTerm);
        updateTerm(nextTerm);
    }

    public void updateTerm(long term) {
        this.term.set(term);
    }

    private long calculateNextTerm(int numberOfServers) {
        long currentTerm = getTermValue();
        currentTerm++;

        while (currentTerm % numberOfServers != getIdValue()) {
            currentTerm++;
        }
        return currentTerm;
    }

    public int calculateServerId(int serverPort, int numberOfServers) {
        log.debug("Generating server id for port: {}", serverPort);
        return serverPort % numberOfServers;
    }

    public long getIdValue() {
        return getId().get();
    }

    public long getTermValue() {
        return getTerm().get();
    }

    public long getCommitIndexValue() {
        return getCommitIndex().get();
    }

    public boolean isLeader() {
        return getLeader().get();
    }

    public void demoteLeader() {
        setLeader(false);
    }

    public void setLeader(boolean leader) {
        getLeader().set(leader);
    }
}
