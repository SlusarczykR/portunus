package org.slusarczykr.portunus.cache.cluster.leader.api.client;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.SyncServerStateRequest;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class PaxosGRPCClient extends AbstractManaged implements PaxosClient {

    private static final Logger log = LoggerFactory.getLogger(PaxosGRPCClient.class);

    private final ClusterService clusterService;
    private final ManagedChannel channel;

    public PaxosGRPCClient(ClusterService clusterService, Address address) {
        super();
        this.clusterService = clusterService;
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    public PaxosGRPCClient(ClusterService clusterService, ManagedChannel channel) {
        super();
        this.clusterService = clusterService;
        this.channel = channel;
    }

    private ManagedChannel initializeManagedChannel(String address, int port) {
        return ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext()
                .build();
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

    private <T extends GeneratedMessageV3> T withPortunusServiceStub(Function<PortunusServiceBlockingStub, T> executable) {
        PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
        return executable.apply(portunusServiceStub);
    }

    private PortunusServiceBlockingStub newPortunusServiceStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    private AddressDTO getLocalServerAddressDTO() {
        LocalPortunusServer localServer = clusterService.getLocalServer();
        return clusterService.getConversionService().convert(localServer.getAddress());
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
