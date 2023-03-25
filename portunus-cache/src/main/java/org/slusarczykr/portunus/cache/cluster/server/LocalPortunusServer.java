package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.maintenance.Managed;
import org.slusarczykr.portunus.cache.manager.CacheManager;
import org.slusarczykr.portunus.cache.manager.DefaultCacheManager;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class LocalPortunusServer extends AbstractPortunusServer implements Managed {

    public static final int DEFAULT_SERVER_PORT = 8090;

    private final CacheManager cacheManager;

    private Server gRPCServer;

    private LocalPortunusServer(ClusterMemberContext context) {
        super(context);
        this.cacheManager = DefaultCacheManager.getInstance();
    }

    public static LocalPortunusServer newInstance() {
        return new LocalPortunusServer(createServerContext());
    }

    @SneakyThrows
    private static ClusterMemberContext createServerContext() {
        ClusterConfig clusterConfig = DefaultClusterService.getInstance().getClusterConfigService().getClusterConfig();
        Address address = clusterConfig.getLocalServerAddress();

        return new ClusterMemberContext(address);
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
    public boolean isLocal() {
        return true;
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name) {
        return null;
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) throws PortunusException {
        return cacheManager.getCache(cacheName).containsKey(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return new HashSet<>(cache.allEntries());
    }
}
