package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;

public abstract class AbstractPaxosService extends AbstractService implements PaxosServerHolder {

    protected PaxosServer paxosServer;

    protected AbstractPaxosService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void setPaxosServer(PaxosServer paxosServer) {
        this.paxosServer = paxosServer;
    }
}
