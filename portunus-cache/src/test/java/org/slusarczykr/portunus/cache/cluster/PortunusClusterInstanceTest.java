package org.slusarczykr.portunus.cache.cluster;

import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.maintenance.DefaultManagedCollector;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PortunusClusterInstanceTest {

    @Test
    void shouldInitializeWhenStarted() {

        PortunusClusterInstance portunusClusterInstance = PortunusClusterInstance.newInstance();
        Collection<Managed> allManagedObjects = DefaultManagedCollector.getInstance().getAllManaged();

        assertNotNull(portunusClusterInstance);
        assertNotNull(portunusClusterInstance.localMember());
    }
}