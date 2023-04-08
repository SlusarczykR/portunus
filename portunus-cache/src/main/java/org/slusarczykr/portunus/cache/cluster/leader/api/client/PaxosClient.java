package org.slusarczykr.portunus.cache.cluster.leader.api.client;


import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

public interface PaxosClient {

    RequestVoteResponse sendRequestVote(long serverId, long term);

    AppendEntryResponse sendHeartbeats(long serverId, long term);
}
