package org.slusarczykr.portunus.cache.cluster.leader.api.client;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.util.Optional;
import java.util.function.Function;

public class PaxosGRPCClient extends AbstractManaged implements PaxosClient {

    private static final Logger log = LoggerFactory.getLogger(PaxosGRPCClient.class);

    private final ManagedChannel channel;

    public PaxosGRPCClient(Address address, PaxosServer paxosServer) {
        this.channel = initializeManagedChannel(address.hostname(), address.port());
    }

    public PaxosGRPCClient(ManagedChannel channel, PaxosServer paxosServer) {
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

    private static AppendEntry createAppendEntry(long serverId, long term) {
        return AppendEntry.newBuilder()
                .setServerId(serverId)
                .setTerm(term)
                .build();
    }

    private <T extends GeneratedMessageV3> T withPortunusServiceStub(Function<PortunusServiceGrpc.PortunusServiceBlockingStub, T> executable) {
        PortunusServiceGrpc.PortunusServiceBlockingStub portunusServiceStub = newPortunusServiceStub();
        return executable.apply(portunusServiceStub);
    }

    private PortunusServiceGrpc.PortunusServiceBlockingStub newPortunusServiceStub() {
        return PortunusServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdown);
    }
}
