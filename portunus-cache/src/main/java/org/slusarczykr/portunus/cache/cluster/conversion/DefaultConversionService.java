package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.PortunusNode;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DefaultConversionService extends AbstractService implements ConversionService {

    private DefaultConversionService(ClusterService clusterService) {
        super(clusterService);
    }

    public static DefaultConversionService newInstance(ClusterService clusterService) {
        return new DefaultConversionService(clusterService);
    }

    @Override
    public Partition convert(PartitionDTO partition) {
        Address address = convert(partition.getOwner());
        return clusterService.getDiscoveryService().getServer(address)
                .map(it -> new Partition((int) partition.getKey(), it, getReplicaOwners(partition)
                ))
                .orElseThrow(() -> new IllegalStateException(String.format("Could not find server with address: '%s'", address)));
    }

    private List<PortunusServer> getReplicaOwners(PartitionDTO partition) {
        return partition.getReplicaOwnersList().stream()
                .map(server -> clusterService.getDiscoveryService().getServer(convert(server)))
                .flatMap(Optional::stream)
                .toList();
    }

    @Override
    public PartitionDTO convert(Partition partition) {
        return PartitionDTO.newBuilder()
                .setKey(partition.getPartitionId())
                .setOwner(convert(partition.getOwnerAddress()))
                .addAllReplicaOwners(convertReplicaOwners(partition.getReplicaOwners()))
                .build();
    }

    private List<AddressDTO> convertReplicaOwners(Set<PortunusServer> replicaOwners) {
        return replicaOwners.stream()
                .map(PortunusServer::getAddress)
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntryDTO cacheEntry) {
        Distributed<K> key = DistributedWrapper.fromBytes(cacheEntry.getKey().toByteArray());
        Distributed<V> value = DistributedWrapper.fromBytes(cacheEntry.getValue().toByteArray());

        return new DefaultCache.Entry<>(key.get(), value.get());
    }

    @Override
    public <K extends Serializable, V extends Serializable> CacheEntryDTO convert(Cache.Entry<K, V> cacheEntry) {
        Distributed<K> key = DistributedWrapper.from(cacheEntry.getKey());
        Distributed<V> value = DistributedWrapper.from(cacheEntry.getValue());

        return CacheEntryDTO.newBuilder()
                .setKey(key.getByteString())
                .setValue(value.getByteString())
                .build();
    }

    @Override
    public Address convert(AddressDTO address) {
        return new Address(address.getHostname(), address.getPort());
    }

    @Override
    public AddressDTO convert(Address address) {
        return AddressDTO.newBuilder()
                .setHostname(address.hostname())
                .setPort(address.port())
                .build();
    }

    @Override
    public RequestVote.Response convert(RequestVoteResponse requestVoteResponse) {
        long serverId = requestVoteResponse.getServerId();
        long term = requestVoteResponse.getTerm();

        if (requestVoteResponse.getAccepted()) {
            return new RequestVote.Response.Accepted(serverId, term);
        }
        return new RequestVote.Response.Rejected(serverId, term);
    }

    @Override
    public RequestVoteResponse convert(RequestVote.Response requestVoteResponse) {
        return RequestVoteResponse.newBuilder()
                .setServerId(requestVoteResponse.getServerId())
                .setTerm(requestVoteResponse.getTerm())
                .setAccepted(requestVoteResponse.isAccepted())
                .build();
    }

    @Override
    public RequestVote convert(AppendEntry appendEntry) {
        return new RequestVote(appendEntry.getServerId(), appendEntry.getTerm());
    }

    @Override
    public VirtualPortunusNodeDTO convert(VirtualPortunusNode virtualPortunusNode) {
        AddressDTO address = convert(Address.from(virtualPortunusNode.getPhysicalNodeKey()));

        return VirtualPortunusNodeDTO.newBuilder()
                .setHashCode(virtualPortunusNode.getKey())
                .setPhysicalNodeAddress(address)
                .setReplicaIndex(virtualPortunusNode.getReplicaIndex())
                .build();
    }

    @Override
    public VirtualPortunusNode convert(VirtualPortunusNodeDTO virtualPortunusNode) {
        Address address = convert(virtualPortunusNode.getPhysicalNodeAddress());
        PortunusNode portunusNode = new PortunusNode(address);
        return new VirtualPortunusNode(portunusNode, (int) virtualPortunusNode.getReplicaIndex());
    }

    @Override
    public String getName() {
        return ConversionService.class.getSimpleName();
    }
}
