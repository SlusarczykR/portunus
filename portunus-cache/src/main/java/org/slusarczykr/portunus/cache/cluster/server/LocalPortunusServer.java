package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.config.DefaultClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.discovery.DefaultDiscoveryService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.net.InetAddress;
import java.util.Optional;

public class LocalPortunusServer extends AbstractPortunusServer implements Managed {

    public static final int DEFAULT_SERVER_PORT = 8090;
    private Server gRPCServer;

    private LocalPortunusServer(ClusterMemberContext context) {
        super(context);
    }

    public static LocalPortunusServer newInstance() {
        return new LocalPortunusServer(createServerContext());
    }

    @SneakyThrows
    private static ClusterMemberContext createServerContext() {
        InetAddress inetAddress = InetAddress.getLocalHost();
        ClusterConfig clusterConfig = DefaultClusterConfigService.getInstance().getClusterConfig();
        Address address = new Address(inetAddress.getHostAddress(), clusterConfig.getPort());

        return new ClusterMemberContext(address);
    }

    @Override
    protected void initialize() throws PortunusException {
        this.gRPCServer = initializeGRPCServer();
        DefaultDiscoveryService.getInstance().loadServers();
    }

    private static Server initializeGRPCServer() {
        return ServerBuilder.forPort(DEFAULT_SERVER_PORT)
                .addService(new PortunusGRPCService())
                .build();
    }

    public final void start() {

    }

    @Override
    public void shutdown() {
        Optional.ofNullable(gRPCServer).ifPresent(Server::shutdown);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String key) {
        return null;
    }
}
