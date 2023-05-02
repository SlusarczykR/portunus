package org.slusarczykr.portunus.cache.cluster.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
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

    private LocalPortunusServer(ClusterService clusterService, ClusterMemberContext context) {
        super(clusterService, context);
    }

    public static LocalPortunusServer newInstance(ClusterService clusterService, ClusterConfig clusterConfig) {
        ClusterMemberContext serverContext = createServerContext(clusterService, clusterConfig);
        return new LocalPortunusServer(clusterService, serverContext);
    }

    @SneakyThrows
    private static ClusterMemberContext createServerContext(ClusterService clusterService, ClusterConfig clusterConfig) {
        clusterConfig = Optional.ofNullable(clusterConfig).orElseGet(() -> getClusterConfig(clusterService));
        Address address = clusterConfig.getLocalServerAddress();
        int numberOfServers = clusterConfig.getMembers().size() + 1;

        return new ClusterMemberContext(address, numberOfServers);
    }

    private static ClusterConfig getClusterConfig(ClusterService clusterService) {
        return clusterService.getClusterConfigService().getClusterConfig();
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
        return ServerBuilder.forPort(serverContext.getPort())
                .addService(new PortunusGRPCService(clusterService))
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
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) {
        return cacheManager.getCache(cacheName).containsKey(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> getCacheEntry(String name, K key) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return cache.getEntry(key).orElse(null);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return new HashSet<>(cache.allEntries());
    }

    @Override
    public <K extends Serializable, V extends Serializable> boolean put(String name, Cache.Entry<K, V> entry) {
        Cache<K, V> cache = cacheManager.getCache(name);

        if (!cache.containsKey(entry.getKey())) {
            cache.put(entry);
            return true;
        }
        return false;
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key) {
        Cache<K, V> cache = cacheManager.getCache(name);
        return cache.remove(key);
    }

    @Override
    public void sendEvent(ClusterEvent event) {
    }
}
