package org.slusarczykr.portunus.cache.cluster.leader.api.client;


import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.util.List;

public interface PaxosClient {

    RequestVoteResponse sendRequestVote(long serverId, long term);

    AppendEntryResponse sendHeartbeats(long serverId, long term);

    AppendEntryResponse sendPartitionMap(long serverId,
                                         List<VirtualPortunusNodeDTO> partitionOwnerCircleNodes,
                                         List<PartitionDTO> partitions);
}
