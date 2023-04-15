package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;

public interface PaxosService {

    void setPaxosServer(PaxosServer paxosServer);
}
