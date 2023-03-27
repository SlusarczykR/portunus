package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PortunusClusterInstanceTest {

    @Test
    void shouldInitializeWhenStarted() {
        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.newInstance();

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }
}