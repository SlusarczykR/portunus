package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.client.PortunusClient;
import org.slusarczykr.portunus.cache.cluster.client.PortunusGRPCClient;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

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
    public <K, V> Cache<K, V> getCache(String key) {
        return null;
    }
}
