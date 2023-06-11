package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

import java.util.Objects;

public abstract class AbstractPortunusServer extends AbstractManaged implements PortunusServer {

    protected final ClusterService clusterService;
    protected final ClusterMemberContext serverContext;
    protected final PaxosServer paxosServer;

    protected AbstractPortunusServer(ClusterService clusterService, ClusterMemberContext serverContext) {
        super(clusterService.getManagedService());
        try {
            this.clusterService = clusterService;
            this.serverContext = serverContext;
            this.paxosServer = new PaxosServer(serverContext.getPort(), serverContext.numberOfServers());
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    protected abstract void initialize() throws PortunusException;

    @Override
    public String getPlainAddress() {
        return serverContext.getPlainAddress();
    }

    @Override
    public Address getAddress() {
        return serverContext.address();
    }

    @Override
    public PaxosServer getPaxosServer() {
        return paxosServer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractPortunusServer that = (AbstractPortunusServer) o;
        return Objects.equals(serverContext, that.serverContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverContext);
    }
}
