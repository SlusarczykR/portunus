package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public abstract class AbstractPortunusServer implements PortunusServer {

    protected final ClusterMemberContext serverContext;

    protected AbstractPortunusServer(ClusterMemberContext serverContext) {
        try {
            this.serverContext = serverContext;
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    protected abstract void initialize() throws PortunusException;

    @Override
    public String getPlainAddress() {
        Address address = serverContext.address();
        return String.format("%s:%s", address.hostname(), address.port());
    }

    @Override
    public Address getAddress() {
        return serverContext.address();
    }
}
