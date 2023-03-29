package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.Cache;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortunusClusterInstanceIntegrationTest {

    @Test
    void shouldInitializeWhenStarted() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }

    @Test
    void shouldCreateDistributedCacheWhenCacheAccessorIsCalled() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);
        Cache<String, String> cache = portunusClusterInstance.getCache("testCache");

        assertNotNull(cache);
        assertTrue(cache.isEmpty());
    }
}