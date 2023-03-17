package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.util.resource.ResourceLoader;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.net.InetAddress;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterDiscoveryConfig.DEFAULT_CONFIG_PATH;

public abstract class AbstractPortunusServer implements PortunusServer {

    protected final ClusterMemberContext serverContext;

    protected AbstractPortunusServer() {
        try {
            this.serverContext = initializeServerContext();
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    protected abstract void initialize() throws PortunusException;

    private ClusterMemberContext initializeServerContext() throws IOException {
        ResourceLoader resourceLoader = YamlResourceLoader.getInstance();
        ClusterDiscoveryConfig clusterDiscoveryConfig = resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterDiscoveryConfig.class);
        InetAddress address = InetAddress.getLocalHost();

        return new ClusterMemberContext(clusterDiscoveryConfig, address.getHostAddress());
    }

    @Override
    public String getAddress() {
        return serverContext.address();
    }
}
