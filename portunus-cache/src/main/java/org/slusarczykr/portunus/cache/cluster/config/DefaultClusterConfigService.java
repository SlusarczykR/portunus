package org.slusarczykr.portunus.cache.cluster.config;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.util.List;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.DEFAULT_CONFIG_PATH;

public class DefaultClusterConfigService implements ClusterConfigService {

    private static final DefaultClusterConfigService INSTANCE = new DefaultClusterConfigService();

    private final ClusterConfig clusterConfig;

    public static DefaultClusterConfigService getInstance() {
        return INSTANCE;
    }

    private DefaultClusterConfigService() {
        try {
            this.clusterConfig = readClusterConfig();
        } catch (IOException e) {
            throw new FatalPortunusException("Cluster configuration could not be loaded");
        }
    }

    @Override
    public String getLocalServerPlainAddress() throws PortunusException {
        return getLocalServerAddress().toPlainAddress();
    }

    @Override
    public Address getLocalServerAddress() throws PortunusException {
        return clusterConfig.getLocalServerAddress();
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    @Override
    public List<Address> getClusterMembers() {
        return clusterConfig.getMembers().stream()
                .map(Address::from)
                .toList();
    }

    private ClusterConfig readClusterConfig() throws IOException {
        YamlResourceLoader resourceLoader = YamlResourceLoader.getInstance();
        return resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterConfig.class);
    }
}
