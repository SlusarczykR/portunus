package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.AbstractManaged;

public abstract class AbstractPortunusServer extends AbstractManaged implements PortunusServer {

    protected final ClusterService clusterService;
    protected final ClusterMemberContext serverContext;
    protected final PaxosServer paxosServer;

    protected AbstractPortunusServer(ClusterService clusterService, ClusterMemberContext serverContext) {
        super();
        try {
            this.clusterService = clusterService;
            this.serverContext = serverContext;
            this.paxosServer = new PaxosServer(serverContext.getPort(), clusterService.getDiscoveryService().allServerAddresses().size());
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
}
