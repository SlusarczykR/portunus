package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.maintenance.Managed;
import org.slusarczykr.portunus.cache.util.resource.ResourceLoader;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig.DEFAULT_CONFIG_PATH;

public class LocalPortunusServer implements PortunusServer, Managed {

    public static final int DEFAULT_SERVER_PORT = 8090;
    private final Server gRPCServer;
    private final ClusterNodeContext serverContext;

    public LocalPortunusServer() {
        try {
            this.gRPCServer = initializeGRPCServer();
            this.serverContext = initializeServerContext();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    private static Server initializeGRPCServer() {
        return ServerBuilder.forPort(DEFAULT_SERVER_PORT)
                .addService(new PortunusGRPCService())
                .build();
    }

    public final void start() {
        try {
            gRPCServer.start();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server failed to start", e);
        }
    }

    private ClusterNodeContext initializeServerContext() throws IOException {
        ResourceLoader resourceLoader = YamlResourceLoader.getInstance();
        ClusterDiscoveryConfig clusterDiscoveryConfig = resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterDiscoveryConfig.class);
        InetAddress address = InetAddress.getLocalHost();

        return new ClusterNodeContext(clusterDiscoveryConfig, address.getHostAddress());
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(gRPCServer).ifPresent(Server::shutdown);
    }

    public record ClusterNodeContext(ClusterDiscoveryConfig clusterDiscoveryConfig, String address) {
    }
}
