package org.slusarczykr.portunus.cache.cluster.server;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.DistributedCache;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PortunusServerProxy implements PortunusServer {

    private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    private PortunusServerProxy() {
    }

    public static PortunusServerProxy newInstance() {
        return new PortunusServerProxy();
    }

    @Override
    public Address getAddress() {
        return null;
    }

    @Override
    public String getPlainAddress() {
        return null;
    }

    @Override
    public <K extends Serializable, V extends Serializable> Cache<K, V> getCache(String name) {
        return (Cache<K, V>) caches.computeIfAbsent(name, it -> new DistributedCache<>(it, Collections.emptyMap()));
    }

    @Override
    public <K extends Serializable> boolean containsEntry(String cacheName, K key) throws PortunusException {
        return Optional.ofNullable(caches.get(cacheName))
                .map(it -> containsKey((Cache<K, ?>) it, key))
                .orElse(false);
    }

    @SneakyThrows
    private static <K extends Serializable> boolean containsKey(Cache<K, ?> it, K key) {
        return it.containsKey(key);
    }

    @Override
    public <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name) {
        Cache<K, V> cache = getCache(name);
        return new HashSet<>(cache.allEntries());
    }
}
