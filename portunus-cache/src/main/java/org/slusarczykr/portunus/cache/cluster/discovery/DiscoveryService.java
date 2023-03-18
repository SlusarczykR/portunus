package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Optional;

public interface DiscoveryService {

    void loadServers() throws PortunusException;

    Optional<PortunusServer> getServer(Address address);

    List<PortunusServer> allServers();

    List<String> allServerAddresses();

    void addServer(PortunusServer server) throws PortunusException;

    void removeServer(Address address) throws PortunusException;
}
