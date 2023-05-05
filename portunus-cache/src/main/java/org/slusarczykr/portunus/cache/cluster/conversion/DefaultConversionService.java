package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.*;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.Distributed;
import org.slusarczykr.portunus.cache.cluster.Distributed.DistributedWrapper;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

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
        PortunusServer owner = clusterService.getDiscoveryService().register(address);

        return newPartition(partition, owner);
    }

    private Partition newPartition(PartitionDTO partition, PortunusServer owner) {
        List<PortunusServer> replicaOwners = getReplicaOwners(partition);
        return new Partition((int) partition.getKey(), owner, replicaOwners);
    }

    private List<PortunusServer> getReplicaOwners(PartitionDTO partition) {
        return partition.getReplicaOwnersList().stream()
                .map(server -> clusterService.getDiscoveryService().register(convert(server)))
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

    @Override
    public <K extends Serializable, V extends Serializable> CacheDTO convert(Cache<K, V> cache) {
        List<CacheEntryDTO> cacheEntries = cache.allEntries().stream()
                .map(this::convert)
                .toList();

        return CacheDTO.newBuilder()
                .setName(cache.getName())
                .addAllCacheEntries(cacheEntries)
                .build();
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> convert(CacheDTO cache) {
        Map<K, V> cacheEntries = cache.getCacheEntriesList().stream()
                .map(it -> (Cache.Entry<K, V>) convert(it))
                .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

        Cache<K, V> newCache = new DefaultCache<>(cache.getName());
        newCache.putAll(cacheEntries);

        return newCache;
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
    public VirtualPortunusNodeDTO convert(Entry<String, VirtualPortunusNode> virtualPortunusNodeEntry) {
        VirtualPortunusNode virtualPortunusNode = virtualPortunusNodeEntry.getValue();
        AddressDTO address = convert(Address.from(virtualPortunusNode.getPhysicalNodeKey()));

        return VirtualPortunusNodeDTO.newBuilder()
                .setHashCode(virtualPortunusNodeEntry.getKey())
                .setPhysicalNodeAddress(address)
                .setReplicaIndex(virtualPortunusNode.getReplicaIndex())
                .build();
    }

    @Override
    public Entry<String, VirtualPortunusNode> convert(VirtualPortunusNodeDTO virtualPortunusNode) {
        Address address = convert(virtualPortunusNode.getPhysicalNodeAddress());
        PortunusNode portunusNode = new PortunusNode(address);

        return Map.entry(
                virtualPortunusNode.getHashCode(),
                new VirtualPortunusNode(portunusNode, (int) virtualPortunusNode.getReplicaIndex())
        );
    }

    @Override
    public CacheChunk convert(CacheChunkDTO cacheChunkDTO) {
        Partition partition = convert(cacheChunkDTO.getPartition());
        Set<Cache<? extends Serializable, ? extends Serializable>> cacheEntries = cacheChunkDTO.getCachesList().stream()
                .map(this::convert)
                .collect(Collectors.toSet());

        return new CacheChunk(partition, cacheEntries);
    }

    @Override
    public CacheChunkDTO convert(CacheChunk cacheChunk) {
        List<CacheDTO> caches = cacheChunk.cacheEntries().stream()
                .map(this::convert)
                .toList();

        return CacheChunkDTO.newBuilder()
                .setPartition(convert(cacheChunk.partition()))
                .addAllCaches(caches)
                .build();
    }

    @Override
    public String getName() {
        return ConversionService.class.getSimpleName();
    }
}
