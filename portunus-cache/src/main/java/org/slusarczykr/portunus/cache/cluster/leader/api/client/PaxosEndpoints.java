package org.slusarczykr.portunus.cache.cluster.leader.api.client;

public class PaxosEndpoints {

    public static final String BASIC = "/leaderElection";
    public static final String HEARTBEAT_PATH = BASIC + "/heartbeat";
    public static final String VOTE_PATH = BASIC + "/vote";

    private PaxosEndpoints() {
    }
}
