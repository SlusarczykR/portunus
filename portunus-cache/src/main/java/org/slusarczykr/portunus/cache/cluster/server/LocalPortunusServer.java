package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos;
import org.slusarczykr.portunus.cache.cluster.DefaultClusterService;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.manager.CacheManager;
import org.slusarczykr.portunus.cache.manager.DefaultCacheManager;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class LocalPortunusServer extends AbstractPortunusServer {

    private static final Logger log = LoggerFactory.getLogger(LocalPortunusServer.class);

    private CacheManager cacheManager;

    private Server gRPCServer;

    private LocalPortunusServer(ClusterMemberContext context) {
        super(context);
    }

    public static LocalPortunusServer newInstance(ClusterConfig clusterConfig) {
        ClusterMemberContext serverContext = createServerContext(clusterConfig);
        return new LocalPortunusServer(serverContext);
    }

    @SneakyThrows
    private static ClusterMemberContext createServerContext(ClusterConfig clusterConfig) {
        clusterConfig = Optional.ofNullable(clusterConfig).orElseGet(LocalPortunusServer::getClusterConfig);
        Address address = clusterConfig.getLocalServerAddress();

        return new ClusterMemberContext(address);
    }

    private static ClusterConfig getClusterConfig() {
        return DefaultClusterService.getInstance().getClusterConfigService().getClusterConfig();
    }

    @Override
    protected void initialize() throws PortunusException {
        try {
            log.info("Starting gRPC server for '{}' server", serverContext.getPlainAddress());
            this.cacheManager = DefaultCacheManager.getInstance();
            this.gRPCServer = createGRPCServer();
            this.gRPCServer.start();
        } catch (IOException e) {
            throw new PortunusException("Could not start gRPC server", e);
        }
    }

    private Server createGRPCServer() {
        return ServerBuilder.forPort(getClusterConfig().getPort())
                .addService(new PortunusGRPCService())
                .build();
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
    public boolean anyEntry(String cacheName) {
        return !cacheManager.getCache(cacheName).isEmpty();
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

    @Override
    public <K extends Serializable, V extends Serializable> boolean put(String name, Cache.Entry<K, V> entry) throws PortunusException {
        Cache<K, V> cache = cacheManager.getCache(name);

        if (!cache.containsKey(entry.getKey())) {
            cache.put(entry);
            return true;
        }
        return false;
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key) throws PortunusException {
        Cache<K, V> cache = cacheManager.getCache(name);
        return cache.remove(key);
    }

    @Override
    public void sendEvent(PortunusEventApiProtos.ClusterEvent event) {

    }
}
