package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.Service;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Optional;

public interface DiscoveryService extends Service {

    void loadServers() throws PortunusException;

    Optional<PortunusServer> getServer(Address address);

    PortunusServer getServerOrThrow(Address address) throws PortunusException;

    List<RemotePortunusServer> remoteServers();

    List<PortunusServer> allServers();

    List<String> allServerAddresses();

    void register(PortunusServer server) throws PortunusException;

    void unregister(Address address) throws PortunusException;
}
