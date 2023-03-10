package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;

public class DefaultPartitionService implements PartitionService {

    private static final DefaultPartitionService INSTANCE = new DefaultPartitionService();
    private final DiscoveryService discoveryService;

    private DefaultPartitionService() {
        this.discoveryService = DefaultDiscoveryService.getInstance();
    }

    public static DefaultPartitionService getInstance() {
        return INSTANCE;
    }

    @Override
    public PortunusServer getPartitionOwner(String key) {
        return null;
    }
}
