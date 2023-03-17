package org.slusarczykr.portunus.cache.cluster.discovery;

import org.slusarczykr.portunus.cache.cluster.partition.DefaultPartitionService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDiscoveryService implements DiscoveryService {

    private static final DefaultDiscoveryService INSTANCE = new DefaultDiscoveryService();

    private final PartitionService partitionService;

    private final Map<String, PortunusServer> portunusInstances = new ConcurrentHashMap<>();

    private DefaultDiscoveryService() {
        this.partitionService = DefaultPartitionService.getInstance();
    }

    public static DefaultDiscoveryService getInstance() {
        return INSTANCE;
    }

    @Override
    public Optional<PortunusServer> getServer(String address) {
        if (portunusInstances.containsKey(address)) {
            return Optional.of(portunusInstances.get(address));
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
        if (portunusInstances.containsKey(server.getAddress())) {
            throw new PortunusException(String.format("Server with address %s already exists", server.getAddress()));
        }
        partitionService.register(server.getAddress());
        portunusInstances.put(server.getAddress(), server);
    }

    @Override
    public void removeServer(String address) throws PortunusException {
        if (!portunusInstances.containsKey(address)) {
            throw new PortunusException(String.format("Server with address %s does not exists", address));
        }
        partitionService.unregister(address);
        portunusInstances.remove(address);
    }
}
