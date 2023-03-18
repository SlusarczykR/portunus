package org.slusarczykr.portunus.cache.cluster.discovery;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.config.DefaultClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDiscoveryService implements DiscoveryService {

    private static final DefaultDiscoveryService INSTANCE = new DefaultDiscoveryService();

    private final PartitionService partitionService;
    private final ClusterConfigService clusterConfigService;

    private final Map<String, PortunusServer> portunusInstances = new ConcurrentHashMap<>();

    private DefaultDiscoveryService() {
        this.partitionService = DefaultPartitionService.getInstance();
        this.clusterConfigService = DefaultClusterConfigService.getInstance();
    }

    public static DefaultDiscoveryService getInstance() {
        return INSTANCE;
    }

    @Override
    public void loadServers() throws PortunusException {
        List<Address> memberAddresses = clusterConfigService.getClusterMembers();
        memberAddresses.forEach(this::loadServer);
    }

    @SneakyThrows
    private void loadServer(Address address) {
        RemotePortunusServer portunusServer = RemotePortunusServer.newInstance(address);
        addServer(portunusServer);
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
    public void addServer(PortunusServer server) throws PortunusException {
        if (portunusInstances.containsKey(server.getPlainAddress())) {
            throw new PortunusException(String.format("Server with address %s already exists", server.getAddress()));
        }
        partitionService.register(server.getAddress());
        portunusInstances.put(server.getPlainAddress(), server);
    }

    @Override
    public void removeServer(Address address) throws PortunusException {
        String plainAddress = address.toPlainAddress();

        if (!portunusInstances.containsKey(plainAddress)) {
            throw new PortunusException(String.format("Server with address %s does not exists", address));
        }
        partitionService.unregister(address);
        portunusInstances.remove(plainAddress);
    }
}
