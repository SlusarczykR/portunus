package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.cluster.client.PortunusGRPCClient;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

public class RemotePortunusServer extends AbstractPortunusServer {

    private PortunusClient portunusClient;

    public static RemotePortunusServer newInstance(Address address) {
        return new RemotePortunusServer(address);
    }

    private RemotePortunusServer(Address address) {
        super(new ClusterMemberContext(address));
    }

    @Override
    protected void initialize() throws PortunusException {
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
                .map(it -> (Cache.Entry<K, V>) conversionService.convert(it))
                .collect(Collectors.toSet());
    }

    @Override
    public void sendEvent(ClusterEvent event) {
        portunusClient.sendEvent(event);
    }

    @Override
    public void shutdown() {
    }
}
