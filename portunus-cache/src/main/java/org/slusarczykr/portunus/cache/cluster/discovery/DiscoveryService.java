package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.List;

public interface DiscoveryService extends Service {

    void loadServers() throws PortunusException;

    PortunusServer getServer(Address address) throws PortunusException;

    PortunusServer getServer(Address address, boolean fresh) throws PortunusException;

    PortunusServer localServer();

    boolean anyRemoteServerAvailable();

    List<RemotePortunusServer> remoteServers();

    List<RemotePortunusServer> remoteServers(boolean fresh);

    List<PortunusServer> allServers();

    List<PortunusServer> allServers(boolean fresh);

    List<String> allServerAddresses(boolean fresh);

    int getNumberOfServers();

    boolean register(PortunusServer server) throws PortunusException;

    PortunusServer registerRemoteServer(Address address);

    List<PortunusServer> registerRemoteServers(Collection<Address> addresses);

    void unregister(Address address) throws PortunusException;

    boolean isLocalAddress(Address address);
}
