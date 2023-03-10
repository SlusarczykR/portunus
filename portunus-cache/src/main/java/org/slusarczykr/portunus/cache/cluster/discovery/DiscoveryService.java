package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

public interface DiscoveryService {

    PortunusServer getServer(String address);
}
