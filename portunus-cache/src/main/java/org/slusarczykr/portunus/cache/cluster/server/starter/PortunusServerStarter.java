package org.slusarczykr.portunus.cache.cluster.server.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.server.LocalPortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.maintenance.DefaultManagedCollector;
import org.slusarczykr.portunus.cache.maintenance.Managed;

import java.util.Collection;

public final class PortunusServerStarter {

    private static final Logger log = LoggerFactory.getLogger(PortunusServerStarter.class);

    private static final PortunusServer server;

    static {
        server = new LocalPortunusServer();
    }

    public static void main(String[] args) {
        addShutdownHook();
    }

    private static void addShutdownHook() {
        Thread serverShutdownHook = new Thread(() -> {
            log.info("Portunus server is shutting down");
            Collection<Managed> managedObjects = DefaultManagedCollector.getInstance().getAllManagedObjects();
            managedObjects.forEach(PortunusServerStarter::shutdownManagedObject);
        });
        Runtime.getRuntime().addShutdownHook(serverShutdownHook);
    }

    private static void shutdownManagedObject(Managed managedObject) {
        try {
            managedObject.shutdown();
        } catch (Exception e) {
            log.error("Error occurred during '{}' shutdown", managedObject.getClass().getSimpleName());
        }
    }
}
