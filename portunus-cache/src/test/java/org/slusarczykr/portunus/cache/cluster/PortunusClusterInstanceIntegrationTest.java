package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.Cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortunusClusterInstanceIntegrationTest {

    private static final String DEFAULT_CACHE_NAME = "testCache";

    @Test
    void shouldInitializeWhenStarted() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }

    @Test
    void shouldCreateDistributedCacheWhenCacheAccessorIsCalled() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        Cache<String, String> cache = portunusClusterInstance.getCache(DEFAULT_CACHE_NAME);

        assertNotNull(cache);
        assertTrue(cache.isEmpty());
    }

    @Test
    void shouldCreateDistributedCacheAndStoreEntryWhenCacheAccessorIsCalled() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        Cache<String, String> cache = portunusClusterInstance.getCache(DEFAULT_CACHE_NAME);
        cache.put("testEntryKey", "testEntryValue");

        assertNotNull(cache);
        assertFalse(cache.isEmpty());
    }
}