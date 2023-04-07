package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slusarczykr.portunus.cache.cluster.PortunusClusterInstance.DEFAULT_PORT;

class PortunusClusterInstanceIntegrationTest {

    private static final String DEFAULT_CACHE_NAME = "testCache";
    private static final String DEFAULT_CACHE_ENTRY_KEY = "testEntryKey";
    private static final String DEFAULT_CACHE_ENTRY_VALUE = "testEntryValue";

    @Test
    void shouldInitializeWhenConfigIsNull() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }

    @Test
    void shouldInitializeWhenConfigIsGiven() {
        ClusterConfig clusterConfig = newClusterConfig();
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(clusterConfig);

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }

    private static ClusterConfig newClusterConfig() {
        return ClusterConfig.builder()
                .port(DEFAULT_PORT)
                .build();
    }

    @Test
    void shouldCreateDistributedCacheWhenCacheAccessOperatorIsCalled() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        Cache<String, String> cache = portunusClusterInstance.getCache(DEFAULT_CACHE_NAME);

        assertNotNull(cache);
        assertTrue(cache.isEmpty());
    }

    @Test
    void shouldCreateDistributedCacheAndStoreEntryWhenPutOperationIsCalled() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);
        Cache<String, String> cache = portunusClusterInstance.getCache(DEFAULT_CACHE_NAME);

        cache.put(DEFAULT_CACHE_ENTRY_KEY, DEFAULT_CACHE_ENTRY_VALUE);

        assertNotNull(cache);
        assertFalse(cache.isEmpty());
    }

    @Test
    void shouldRemoveEntryFromCacheWhenRemoveOperationIsCalled() throws PortunusException {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);
        Cache<String, String> cache = portunusClusterInstance.getCache(DEFAULT_CACHE_NAME);
        cache.put(DEFAULT_CACHE_ENTRY_KEY, DEFAULT_CACHE_ENTRY_VALUE);

        assertTrue(cache.containsKey(DEFAULT_CACHE_ENTRY_KEY));
        cache.remove(DEFAULT_CACHE_ENTRY_KEY);

        assertNotNull(cache);
        assertFalse(cache.containsKey(DEFAULT_CACHE_ENTRY_KEY));
        assertTrue(cache.isEmpty());
    }

    @Test
    void shouldReturnEntryWhenRemoteEntryIsRequested() throws PortunusException {
        Address localAddress = new Address("localhost", DEFAULT_PORT);
        Address remoteAddress = new Address("localhost", 8092);
        PortunusClusterInstance localPortunusInstance = PortunusClusterInstance.newInstance(createClusterConfig(localAddress.port(), List.of(remoteAddress.toPlainAddress())));
        PortunusClusterInstance remotePortunusInstance = PortunusClusterInstance.newInstance(createClusterConfig(remoteAddress.port(), List.of(localAddress.toPlainAddress())));
        Cache<String, String> cache = localPortunusInstance.getCache(DEFAULT_CACHE_NAME);
        cache.put(DEFAULT_CACHE_ENTRY_KEY, DEFAULT_CACHE_ENTRY_VALUE);

        assertTrue(cache.containsKey(DEFAULT_CACHE_ENTRY_KEY));
        cache.remove(DEFAULT_CACHE_ENTRY_KEY);

        assertNotNull(cache);
        assertFalse(cache.containsKey(DEFAULT_CACHE_ENTRY_KEY));
        assertTrue(cache.isEmpty());
    }

    private ClusterConfig createClusterConfig(int port, List<String> members) {
        return ClusterConfig.builder()
                .port(port)
                .members(members)
                .build();
    }
}