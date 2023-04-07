package org.slusarczykr.portunus.cache.cluster.discovery;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class DefaultDiscoveryService extends AbstractService implements DiscoveryService {

    private final Map<String, PortunusServer> portunusInstances = new ConcurrentHashMap<>();

    private DefaultDiscoveryService(ClusterService clusterService) {
        super(clusterService);
    }

    public static DefaultDiscoveryService newInstance(ClusterService clusterService) {
        return new DefaultDiscoveryService(clusterService);
    }

    @Override
    public void onInitialization() throws PortunusException {
        loadServers();
    }

    @Override
    public void loadServers() throws PortunusException {
        List<Address> memberAddresses = clusterService.getClusterConfigService().getClusterMembers();
        memberAddresses.forEach(this::loadServer);
    }

    @SneakyThrows
    private void loadServer(Address address) {
        RemotePortunusServer portunusServer = RemotePortunusServer.newInstance(clusterService, address);
        register(portunusServer);
    }

    @Override
    public Optional<PortunusServer> getServer(Address address) {
        String plainAddress = address.toPlainAddress();

        if (portunusInstances.containsKey(plainAddress)) {
            return Optional.of(portunusInstances.get(plainAddress));
        }
        return Optional.empty();
    }

    @Override
    public PortunusServer getServerOrThrow(Address address) throws PortunusException {
        return getServer(address)
                .orElseThrow(() -> new PortunusException(String.format("Server: %s could not be found", address.toPlainAddress())));
    }

    @Override
    public PortunusServer localServer() {
        return portunusInstances.values().stream()
                .filter(PortunusServer::isLocal)
                .findFirst()
                .orElseThrow(() -> new InvalidPortunusStateException("Could not access local server"));
    }

    @Override
    public List<RemotePortunusServer> remoteServers() {
        return portunusInstances.values().stream()
                .filter(Predicate.not(PortunusServer::isLocal).and(RemotePortunusServer.class::isInstance))
                .map(RemotePortunusServer.class::cast)
                .toList();
    }

    @Override
    public List<PortunusServer> allServers() {
        return portunusInstances.values().stream()
                .toList();
    }

    @Override
    public List<String> allServerAddresses() {
        return portunusInstances.keySet().stream()
                .toList();
    }

    @Override
    public void register(PortunusServer server) throws PortunusException {
        if (portunusInstances.containsKey(server.getPlainAddress())) {
            throw new PortunusException(String.format("Server with address %s already exists", server.getAddress()));
        }
        clusterService.getPartitionService().register(server.getAddress());
        portunusInstances.put(server.getPlainAddress(), server);
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        String plainAddress = address.toPlainAddress();

        if (!portunusInstances.containsKey(plainAddress)) {
            throw new PortunusException(String.format("Server with address %s does not exists", address));
        }
        clusterService.getPartitionService().unregister(address);
        portunusInstances.remove(plainAddress);
    }

    @Override
    public String getName() {
        return DiscoveryService.class.getSimpleName();
    }
}
