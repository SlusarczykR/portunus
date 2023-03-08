package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig;
import org.slusarczykr.portunus.cache.util.resource.ResourceLoader;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.net.InetAddress;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig.DEFAULT_CONFIG_PATH;

public class DefaultPortunusServer implements PortunusServer {

    private final ClusterNodeContext serverContext;

    public DefaultPortunusServer() throws IOException {
        serverContext = initializeServerContext();
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
