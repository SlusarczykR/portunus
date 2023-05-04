package org.slusarczykr.portunus.cache.cluster.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DefaultCache;
import org.slusarczykr.portunus.cache.DistributedCache.OperationType;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheChunkDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.PartitionEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.chunk.CacheChunk;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.cluster.client.PortunusGRPCClient;
import org.slusarczykr.portunus.cache.cluster.leader.api.client.PaxosClient;
import org.slusarczykr.portunus.cache.cluster.leader.api.client.PaxosGRPCClient;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntryResponse;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RemotePortunusServer extends AbstractPortunusServer implements PaxosClient {

    private static final Logger log = LoggerFactory.getLogger(RemotePortunusServer.class);

    private PortunusClient portunusClient;
    private PaxosClient paxosClient;

    public static RemotePortunusServer newInstance(ClusterService clusterService, ClusterMemberContext context) {
        return new RemotePortunusServer(clusterService, context);
    }

    private RemotePortunusServer(ClusterService clusterService, ClusterMemberContext context) {
        super(clusterService, context);
    }

    @Override
    protected void initialize() throws PortunusException {
        log.info("Initializing gRPC client for '{}' remote server", serverContext.getPlainAddress());
        this.portunusClient = new PortunusGRPCClient(serverContext.address());
        this.paxosClient = new PaxosGRPCClient(serverContext.address());
    }

    @Override
    public boolean anyEntry(String cacheName) {
        return portunusClient.anyEntry(cacheName);
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        return portunusClient.containsEntry(cacheName, key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name) {
        Map<K, V> cacheEntries = getCacheEntries(name).stream()
                .collect(Collectors.toMap(it -> (K) it.getKey(), it -> (V) it.getValue()));
        Cache<K, V> cache = newdCache(name);
        cache.putAll(cacheEntries);

        return cache;
    }

    private <K extends Serializable, V extends Serializable> Cache<K, V> newdCache(String name) {
        return new DefaultCache<>(name);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> getCacheEntry(String name, K key) {
        CacheEntryDTO cacheEntryDTO = portunusClient.getCacheEntry(name, key);
        return clusterService.getConversionService().convert(cacheEntryDTO);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        return portunusClient.getCache(name).stream()
                .map(it -> (Cache.Entry<K, V>) clusterService.getConversionService().convert(it))
                .collect(Collectors.toSet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> void put(String name, int partitionId, Cache.Entry<K, V> entry) {
        log.info("Sending '{}' operation to {} server", OperationType.SEND_CLUSTER_EVENT, getPlainAddress());
        portunusClient.putEntry(name, clusterService.getConversionService().convert(entry));
    }

    @Override
    public <K extends Serializable, V extends Serializable> void putAll(String name, int partitionId, Map<K, V> entries) {
        List<CacheEntryDTO> cacheEntries = entries.entrySet().stream()
                .map(it -> new DefaultCache.Entry<>(it.getKey(), it.getValue()))
                .map(it -> clusterService.getConversionService().convert(it))
                .toList();

        portunusClient.putEntries(name, cacheEntries);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key) {
        return clusterService.getConversionService().convert(portunusClient.removeEntry(name, key));
    }

    @Override
    public Set<Cache<? extends Serializable, ? extends Serializable>> getCacheEntries(int partitionId) {
        CacheChunkDTO cacheChunkDTO = portunusClient.getCacheChunk(partitionId);
        CacheChunk cacheChunk = clusterService.getConversionService().convert(cacheChunkDTO);

        return cacheChunk.cacheEntries();
    }

    @Override
    public void sendEvent(ClusterEvent event) {
        portunusClient.sendEvent(event);
    }

    @Override
    public void sendEvent(PartitionEvent event) {
        portunusClient.sendEvent(event);
    }

    @Override
    public void replicate(Partition partition) {
        Set<Cache<? extends Serializable, ? extends Serializable>> cacheEntries = clusterService.getLocalMember()
                .getCacheEntries(partition.getPartitionId());
        CacheChunk cacheChunk = new CacheChunk(partition, cacheEntries);
        CacheChunkDTO cacheChunkDTO = clusterService.getConversionService().convert(cacheChunk);

        portunusClient.replicate(cacheChunkDTO);
    }

    @Override
    public RequestVoteResponse sendRequestVote(long serverId, long term) {
        return paxosClient.sendRequestVote(serverId, term);
    }

    @Override
    public AppendEntryResponse sendHeartbeats(long serverId, long term) {
        return paxosClient.sendHeartbeats(serverId, term);
    }

    @Override
    public AppendEntryResponse syncServerState(long serverId,
                                               List<VirtualPortunusNodeDTO> partitionOwnerCircleNodes,
                                               List<PartitionDTO> partitions) {
        return paxosClient.syncServerState(serverId, partitionOwnerCircleNodes, partitions);
    }

    @Override
    public void shutdown() {
    }
}
