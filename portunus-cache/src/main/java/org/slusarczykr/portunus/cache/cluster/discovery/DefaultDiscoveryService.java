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
        withWriteLock(() -> {
            List<Address> memberAddresses = clusterService.getClusterConfigService().getClusterMembers();
            memberAddresses.forEach(this::registerRemoteServer);
        });
    }

    @SneakyThrows
    private RemotePortunusServer registerRemoteServer(Address address) {
        RemotePortunusServer portunusServer = createRemoteServer(address);
        registerServer(portunusServer);

        return portunusServer;
    }

    private void validateRemoteAddress(Address address) {
        Address localServerAddress = clusterService.getLocalServer().getAddress();

        if (address.equals(localServerAddress)) {
            throw new InvalidPortunusStateException(
                    String.format("Could not register remote server. '%s' is local server address", address)
            );
        }
    }

    private RemotePortunusServer createRemoteServer(Address address) {
        validateRemoteAddress(address);
        int numberOfClusterMembers = clusterService.getClusterConfigService().getNumberOfClusterMembers();
        ClusterMemberContext context = new ClusterMemberContext(address, numberOfClusterMembers + 1);

        return RemotePortunusServer.newInstance(clusterService, context);
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
    public PortunusServer register(Address address) {
        return withWriteLock(() ->
                portunusInstances.computeIfAbsent(address.toPlainAddress(), it -> createRemoteServer(address))
        );
    }

    @Override
    public void register(PortunusServer server) throws PortunusException {
        withWriteLock(() -> registerServer(server));
    }

    @SneakyThrows
    private void registerServer(PortunusServer server) {
        if (!portunusInstances.containsKey(server.getPlainAddress())) {
            log.info("Registering server with address: '{}'", server.getPlainAddress());
            portunusInstances.put(server.getPlainAddress(), server);
            clusterService.getPartitionService().register(server);
            log.debug("Current portunus instances: '{}'", portunusInstances);
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
        portunusInstances.remove(plainAddress);
        clusterService.getPartitionService().unregister(address);
    }

    @Override
    public List<PortunusServer> register(Collection<Address> addresses) {
        return withWriteLock(() -> {
            log.debug("Updating portunus cluster members");
            Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();
            return registerServers(addresses, it -> shouldRegisterServer(localServerAddress, it));
        });
    }

    private boolean shouldRegisterServer(Address localServerAddress, Address address) {
        return !(address.equals(localServerAddress) || portunusInstances.containsKey(address.toPlainAddress()));
    }

    private List<PortunusServer> registerServers(Collection<Address> addresses, Predicate<Address> filter) {
        return addresses.stream()
                .filter(filter)
                .map(this::registerRemoteServer)
                .map(PortunusServer.class::cast)
                .toList();
    }

    @Override
    public String getName() {
        return DiscoveryService.class.getSimpleName();
    }
}
