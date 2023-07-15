package org.slusarczykr.portunus.cache.maintenance;

@FunctionalInterface
public interface Managed {

    void shutdown();
}
