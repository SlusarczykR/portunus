package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortunusClusterInstanceIntegrationTest {

    private static final String DEFAULT_CACHE_NAME = "testCache";
    private static final String DEFAULT_CACHE_ENTRY_KEY = "testEntryKey";
    private static final String DEFAULT_CACHE_ENTRY_VALUE = "testEntryValue";

    @Test
    void shouldInitializeWhenStarted() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.getInstance(null);

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
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
}