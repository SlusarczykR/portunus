package org.slusarczykr.portunus.cache.cluster.discovery;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
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
import java.util.function.Supplier;

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
            memberAddresses.forEach(this::registerRemoteAddress);
        });
    }

    @Override
    public PortunusServer getServer(Address address) throws PortunusException {
        return getServer(address, true);
    }

    @SneakyThrows
    private RemotePortunusServer registerRemoteAddress(Address address) {
        validateRemoteAddress(address);
        RemotePortunusServer portunusServer = createRemoteServer(address);
        registerServer(portunusServer);

        return portunusServer;
    }

    private void validateRemoteAddress(Address address) {
        if (isLocalAddress(address)) {
            throw new InvalidPortunusStateException(
                    String.format("Could not register remote server. '%s' is local server address", address)
            );
        }
    }

    private RemotePortunusServer createRemoteServer(Address address) {
        ClusterMemberContext context = new ClusterMemberContext(address);
        return RemotePortunusServer.newInstance(clusterService, context);
    }

    @Override
    public PortunusServer getServer(Address address, boolean fresh) throws PortunusException {
        return findServer(address, fresh)
                .orElseThrow(() -> new PortunusException(String.format("Server: %s could not be found", address)));
    }

    private Optional<PortunusServer> findServer(Address address, boolean fresh) {
        String plainAddress = address.toPlainAddress();

        if (fresh) {
            return withReadLock(() -> Optional.ofNullable(portunusInstances.get(plainAddress)));
        }
        return Optional.ofNullable(portunusInstances.get(plainAddress));
    }

    @Override
    public PortunusServer localServer() {
        return clusterService.getLocalServer();
    }

    @Override
    public boolean anyRemoteServerAvailable() {
        return !remoteServers(false).isEmpty();
    }

    @Override
    public List<RemotePortunusServer> remoteServers() {
        return remoteServers(false);
    }

    public List<RemotePortunusServer> remoteServers(boolean fresh) {
        return withReadLockWrapper(fresh, () -> portunusInstances.values().stream()
                .filter(Predicate.not(PortunusServer::isLocal).and(RemotePortunusServer.class::isInstance))
                .map(RemotePortunusServer.class::cast)
                .toList());
    }

    @Override
    public List<PortunusServer> allServers() {
        return allServers(false);
    }

    public List<PortunusServer> allServers(boolean fresh) {
        return withReadLockWrapper(fresh, () -> portunusInstances.values().stream().toList());
    }

    @Override
    public List<String> allServerAddresses(boolean fresh) {
        if (fresh) {
            return withReadLock(() -> portunusInstances.keySet().stream()
                    .toList());
        }
        return portunusInstances.keySet().stream()
                .toList();
    }

    @Override
    public int getNumberOfServers() {
        return withReadLock(portunusInstances::size);
    }

    @Override
    public PortunusServer registerRemoteServer(Address address) {
        return registerRemoteServer(address, true);
    }

    @Override
    public PortunusServer registerRemoteServer(Address address, boolean lock) {
        return withWriteLockWrapper(lock, () ->
                portunusInstances.computeIfAbsent(address.toPlainAddress(), it -> {
                    log.info("Registering server with address: '{}'", address);
                    return createServer(address);
                })
        );
    }

    @Override
    public boolean isRegistered(Address address) {
        return portunusInstances.containsKey(address.toPlainAddress());
    }

    private PortunusServer createServer(Address address) {
        if (isLocalAddress(address)) {
            return localServer();
        }
        return createRemoteServer(address);
    }

    @Override
    public boolean register(PortunusServer server) throws PortunusException {
        return withWriteLock(() -> registerServer(server));
    }

    @SneakyThrows
    private boolean registerServer(PortunusServer server) {
        if (!portunusInstances.containsKey(server.getPlainAddress())) {
            log.info("Registering server with address: '{}'", server.getPlainAddress());
            portunusInstances.put(server.getPlainAddress(), server);
            clusterService.getPartitionService().register(server);
            ((LocalPortunusServer) localServer()).updatePaxosServerId(portunusInstances.size());
            log.debug("Current portunus instances: '{}'", portunusInstances);
            return true;
        }
        return false;
    }

    @Override
    public void unregister(Address address) throws PortunusException {
        withWriteLock(() -> unregisterServer(address));
    }

    @Override
    public boolean isLocalAddress(Address address) {
        Address localServerAddress = localServer().getAddress();
        return address.equals(localServerAddress);
    }

    @SneakyThrows
    private void unregisterServer(Address address) {
        String plainAddress = address.toPlainAddress();

        if (portunusInstances.containsKey(plainAddress)) {
            log.info("Unregistering remote server with address: '{}'", address);
            portunusInstances.remove(plainAddress);
            clusterService.getLocalServer().updatePaxosServerId(portunusInstances.size());
            clusterService.getPartitionService().unregister(address);
        }
    }

    @Override
    public List<PortunusServer> registerRemoteServers(Collection<Address> addresses) {
        return withWriteLock(() -> {
            log.debug("Updating portunus cluster members");
            Address localServerAddress = clusterService.getClusterConfig().getLocalServerAddress();
            return registerRemoteServers(addresses, it -> shouldRegisterServer(localServerAddress, it), true);
        });
    }

    private boolean shouldRegisterServer(Address localServerAddress, Address address) {
        return !(address.equals(localServerAddress) || portunusInstances.containsKey(address.toPlainAddress()));
    }

    private List<PortunusServer> registerRemoteServers(Collection<Address> addresses, Predicate<Address> filter, boolean lock) {
        return addresses.stream()
                .filter(filter)
                .map(it -> registerRemoteServer(it, lock))
                .toList();
    }

    private <T> T withReadLockWrapper(boolean lock, Supplier<T> operation) {
        if (lock) {
            return withReadLock(operation);
        }
        return operation.get();
    }

    private <T> T withWriteLockWrapper(boolean lock, Supplier<T> operation) {
        if (lock) {
            return withWriteLock(operation);
        }
        return operation.get();
    }

    @Override
    public String getName() {
        return DiscoveryService.class.getSimpleName();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
