package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Optional;

public interface DiscoveryService {

    Optional<PortunusServer> getServer(String address);

    List<PortunusServer> allServers();

    void addServer(PortunusServer server) throws PortunusException;

    void removeServer(String address) throws PortunusException;
}
