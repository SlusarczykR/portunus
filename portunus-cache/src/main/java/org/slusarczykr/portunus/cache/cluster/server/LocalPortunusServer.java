package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Optional;

public class LocalPortunusServer extends AbstractPortunusServer implements Managed {

    public static final int DEFAULT_SERVER_PORT = 8090;
    private Server gRPCServer;

    public LocalPortunusServer() {
        super();
    }

    @Override
    protected void initialize() throws PortunusException {
        this.gRPCServer = initializeGRPCServer();
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
    public <K, V> Cache.Entry<K, V> getEntry(String key) {
        return null;
    }
}
