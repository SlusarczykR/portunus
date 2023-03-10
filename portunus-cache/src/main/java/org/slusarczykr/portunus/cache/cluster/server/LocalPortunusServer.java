package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.util.resource.ResourceLoader;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.net.InetAddress;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig.DEFAULT_CONFIG_PATH;

public class LocalPortunusServer implements PortunusServer {

    private final ClusterNodeContext serverContext;

    public LocalPortunusServer() {
        try {
            serverContext = initializeServerContext();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    public final void start() {
        try {
            //TODO add startup logic
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server failed to start", e);
        }
    }

    private ClusterNodeContext initializeServerContext() throws IOException {
        ResourceLoader resourceLoader = YamlResourceLoader.getInstance();
        ClusterDiscoveryConfig clusterDiscoveryConfig = resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterDiscoveryConfig.class);
        InetAddress address = InetAddress.getLocalHost();

        return new ClusterNodeContext(clusterDiscoveryConfig, address.getHostAddress());
    }

    public record ClusterNodeContext(ClusterDiscoveryConfig clusterDiscoveryConfig, String address) {
    }
}
