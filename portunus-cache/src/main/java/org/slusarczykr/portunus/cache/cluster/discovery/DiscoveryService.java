package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface DiscoveryService extends Service {

    void loadServers() throws PortunusException;

    Optional<PortunusServer> getServer(Address address);

    PortunusServer getServerOrThrow(Address address) throws PortunusException;

    PortunusServer localServer();

    boolean anyRemoteServerAvailable();

    List<RemotePortunusServer> remoteServers();

    List<PortunusServer> allServers();

    List<String> allServerAddresses();

    int getNumberOfServers();

    void register(PortunusServer server) throws PortunusException;

    void unregister(Address address) throws PortunusException;

    void update(Collection<Address> addresses);
}
