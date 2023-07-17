package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;

import java.util.List;

public interface ServiceManager {

    boolean isInitialized();

    void loadServices();

    List<Service> getServices();

    <T extends Service> T getService(Class<T> clazz);

    void injectPaxosServer(PaxosServer paxosServer);
}
