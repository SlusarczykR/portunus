package org.slusarczykr.portunus.cache.cluster.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.cluster.client.PortunusGRPCClient;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

public class RemotePortunusServer extends AbstractPortunusServer {

    private static final Logger log = LoggerFactory.getLogger(RemotePortunusServer.class);

    private PortunusClient portunusClient;

    public static RemotePortunusServer newInstance(ClusterService clusterService, Address address) {
        return new RemotePortunusServer(clusterService, address);
    }

    private RemotePortunusServer(ClusterService clusterService, Address address) {
        super(clusterService, new ClusterMemberContext(address));
    }

    @Override
    protected void initialize() throws PortunusException {
        log.info("Initializing gRPC client for '{}' remote server", serverContext.getPlainAddress());
        this.portunusClient = new PortunusGRPCClient(serverContext.address());
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
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        return portunusClient.getCache(name).stream()
                .map(it -> (Cache.Entry<K, V>) clusterService.getConversionService().convert(it))
                .collect(Collectors.toSet());
    }

    @Override
    public <K extends Serializable, V extends Serializable> boolean put(String name, Cache.Entry<K, V> entry) throws PortunusException {
        return portunusClient.putEntry(name, clusterService.getConversionService().convert(entry));
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key) throws PortunusException {
        return clusterService.getConversionService().convert(portunusClient.removeEntry(name, key));
    }

    @Override
    public void sendEvent(ClusterEvent event) {
        portunusClient.sendEvent(event);
    }

    @Override
    public void shutdown() {
    }
}
