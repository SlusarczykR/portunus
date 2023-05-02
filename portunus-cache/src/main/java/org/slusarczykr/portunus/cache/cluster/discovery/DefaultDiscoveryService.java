package org.slusarczykr.portunus.cache.cluster.discovery;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.RemotePortunusServer;
import org.slusarczykr.portunus.cache.cluster.service.AbstractConcurrentService;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class DefaultDiscoveryService extends AbstractConcurrentService implements DiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(DefaultDiscoveryService.class);

    private final Map<String, PortunusServer> portunusInstances = new ConcurrentHashMap<>();

    private DefaultDiscoveryService(ClusterService clusterService) {
        super(clusterService);
    }

    public static DefaultDiscoveryService newInstance(ClusterService clusterService) {
        return new DefaultDiscoveryService(clusterService);
    }

    @Override
    public void onInitialization() throws PortunusException {
        super.onInitialization();
        loadServers();
    }

    @Override
    public void loadServers() throws PortunusException {
        List<Address> memberAddresses = clusterService.getClusterConfigService().getClusterMembers();
        memberAddresses.forEach(this::register);
    }

    @SneakyThrows
    private void register(Address address) {
        int numberOfClusterMembers = clusterService.getClusterConfigService().getNumberOfClusterMembers();
        ClusterMemberContext context = new ClusterMemberContext(address, numberOfClusterMembers + 1);
        RemotePortunusServer portunusServer = RemotePortunusServer.newInstance(clusterService, context);
        register(portunusServer);
    }

    @Override
    public Optional<PortunusServer> getServer(Address address) {
        String plainAddress = address.toPlainAddress();

        return withReadLock(() -> {
            if (portunusInstances.containsKey(plainAddress)) {
                return Optional.of(portunusInstances.get(plainAddress));
            }
            return Optional.empty();
        });
    }

    @Override
    public PortunusServer getServerOrThrow(Address address) throws PortunusException {
        return getServer(address)
                .orElseThrow(() -> new PortunusException(String.format("Server: %s could not be found", address)));
    }

    @Override
    public PortunusServer localServer() {
        return withReadLock(() -> portunusInstances.values().stream()
                .filter(PortunusServer::isLocal)
                .findFirst()
                .orElseThrow(() -> new InvalidPortunusStateException("Could not access local server"))
        );
    }

    @Override
    public boolean anyRemoteServerAvailable() {
        return !remoteServers().isEmpty();
    }

    @Override
    public List<RemotePortunusServer> remoteServers() {
        return withReadLock(() -> portunusInstances.values().stream()
                .filter(Predicate.not(PortunusServer::isLocal).and(RemotePortunusServer.class::isInstance))
                .map(RemotePortunusServer.class::cast)
                .toList());
    }

    @Override
    public List<PortunusServer> allServers() {
        return withReadLock(() -> portunusInstances.values().stream()
                .toList());
    }

    @Override
    public List<String> allServerAddresses() {
        return withReadLock(() -> portunusInstances.keySet().stream()
                .toList());
    }

    @Override
    public int getNumberOfServers() {
        return withReadLock(portunusInstances::size);
    }

    @Override
    public void register(PortunusServer server) throws PortunusException {
        withWriteLock(() -> registerServer(server));
    }

    @SneakyThrows
    private void registerServer(PortunusServer server) {
        if (!portunusInstances.containsKey(server.getPlainAddress())) {
            log.info("Registering remote server with address: '{}'", server.getPlainAddress());
            clusterService.getPartitionService().register(server.getAddress());
            portunusInstances.put(server.getPlainAddress(), server);
            log.info("Portunus instance map: {}", portunusInstances);
        }
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> unregisterServer(address));
    }

    @SneakyThrows
    private void unregisterServer(Address address) {
        String plainAddress = address.toPlainAddress();

        if (!portunusInstances.containsKey(plainAddress)) {
            throw new PortunusException(String.format("Server with address %s does not exists", address));
        }
        log.info("Unregistering remote server with address: '{}'", address);
        clusterService.getPartitionService().unregister(address);
        portunusInstances.remove(plainAddress);
    }

    @Override
    public void update(Collection<Address> addresses) {
        withWriteLock(() -> {
            log.info("Start updating portunus instance map: {}", portunusInstances);
            Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();
            String localServerPlainAddress = localServerAddress.toPlainAddress();
            removeRemoteServers(portunusInstances, it -> !it.equals(localServerPlainAddress));
            registerAll(addresses, it -> !it.equals(localServerAddress));
            log.info("Portunus instance map was updated");
            log.info("Portunus instance map: {}", portunusInstances);
        });
    }

    private static void removeRemoteServers(Map<String, PortunusServer> portunusServers, Predicate<String> condition) {
        portunusServers.keySet().removeIf(condition);
    }

    private void registerAll(Collection<Address> addresses, Predicate<Address> filter) {
        addresses.stream()
                .filter(filter)
                .forEach(this::register);
    }

    @Override
    public String getName() {
        return DiscoveryService.class.getSimpleName();
    }
}
