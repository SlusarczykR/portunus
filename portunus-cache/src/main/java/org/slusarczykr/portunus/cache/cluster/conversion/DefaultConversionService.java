package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.io.Serializable;

public class DefaultConversionService extends AbstractService implements ConversionService {

    private DefaultConversionService(ClusterService clusterService) {
        super(clusterService);
    }

    public static DefaultConversionService newInstance(ClusterService clusterService) {
        return new DefaultConversionService(clusterService);
    }

    @Override
    public Partition convert(PortunusApiProtos.PartitionDTO partition) {
        return clusterService.getPartitionService().getPartition((int) partition.getKey());
    }

    @Override
    public PortunusApiProtos.PartitionDTO convert(Partition partition) {
        return PortunusApiProtos.PartitionDTO.newBuilder()
                .setKey(partition.partitionId())
                .build();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntryDTO cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.fromBytes(cacheEntry.getKey().toByteArray());
        Distributed<V> value = Distributed.DistributedWrapper.fromBytes(cacheEntry.getValue().toByteArray());

        return new DefaultCache.Entry<>(key.get(), value.get());
    }

    @Override
    public <K extends Serializable, V extends Serializable> CacheEntryDTO convert(Cache.Entry<K, V> cacheEntry) {
        Distributed<K> key = Distributed.DistributedWrapper.from(cacheEntry.getKey());
        Distributed<V> value = Distributed.DistributedWrapper.from(cacheEntry.getValue());

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
    public String getName() {
        return ConversionService.class.getSimpleName();
    }
}
