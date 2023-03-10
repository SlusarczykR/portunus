package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

public class DefaultDiscoveryService implements DiscoveryService {

    private static final DefaultDiscoveryService INSTANCE = new DefaultDiscoveryService();

    private DefaultDiscoveryService() {
    }

    public static DefaultDiscoveryService getInstance() {
        return INSTANCE;
    }

    @Override
    public PortunusServer getServer(String address) {
        return null;
    }
}
