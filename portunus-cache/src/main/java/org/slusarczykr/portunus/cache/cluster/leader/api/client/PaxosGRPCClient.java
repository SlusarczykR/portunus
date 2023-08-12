package org.slusarczykr.portunus.cache.cluster.leader.api.client;

import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.client.AbstractClient;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.SyncServerStateRequest;

import java.util.List;

public class PaxosGRPCClient extends AbstractClient implements PaxosClient {

    private static final Logger log = LoggerFactory.getLogger(PaxosGRPCClient.class);

    public PaxosGRPCClient(ClusterService clusterService, Address address) {
        super(clusterService, address);
    }

    public PaxosGRPCClient(ClusterService clusterService, Address address, ManagedChannel channel) {
        super(clusterService, address, channel);
    }

    @Override
    public RequestVoteResponse sendRequestVote(long serverId, long term) {
        return withPortunusServiceStub(portunusService -> {
            AppendEntry command = createAppendEntry(serverId, term);
            return portunusService.sendRequestVote(command);
        });
    }

    @Override
    public AppendEntryResponse sendHeartbeats(long serverId, long term) {
        return withPortunusServiceStub(portunusService -> {
            AppendEntry command = createAppendEntry(serverId, term);
            return portunusService.sendHeartbeats(command);
        });
    }

    private AppendEntry createAppendEntry(long serverId, long term) {
        return AppendEntry.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setServerId(serverId)
                .setTerm(term)
                .build();
    }

    @Override
    public AppendEntryResponse syncServerState(long serverId,
                                               List<VirtualPortunusNodeDTO> partitionOwnerCircleNodes,
                                               List<PartitionDTO> partitions) {
        return withPortunusServiceStub(portunusService -> {
            SyncServerStateRequest request = createSyncServerStateRequest(serverId, partitionOwnerCircleNodes, partitions);
            return portunusService.syncServerState(request);
        });
    }

    private SyncServerStateRequest createSyncServerStateRequest(long serverId,
                                                                List<VirtualPortunusNodeDTO> partitionOwnerCircleNodes,
                                                                List<PartitionDTO> partitions) {
        return SyncServerStateRequest.newBuilder()
                .setFrom(getLocalServerAddressDTO())
                .setServerId(serverId)
                .addAllVirtualPortunusNode(partitionOwnerCircleNodes)
                .addAllPartition(partitions)
                .build();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
