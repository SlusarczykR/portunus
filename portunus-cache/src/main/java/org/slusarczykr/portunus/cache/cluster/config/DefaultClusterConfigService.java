package org.slusarczykr.portunus.cache.cluster.config;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.util.List;
import java.util.Optional;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.DEFAULT_CONFIG_PATH;

public class DefaultClusterConfigService implements ClusterConfigService {

    private static final DefaultClusterConfigService INSTANCE = new DefaultClusterConfigService();

    private ClusterConfig clusterConfig;

    public static DefaultClusterConfigService getInstance() {
        return INSTANCE;
    }

    private DefaultClusterConfigService() {
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return Optional.ofNullable(clusterConfig)
                .orElseGet(this::readClusterConfig);
    }

    @Override
    public List<PortunusServer.ClusterMemberContext.Address> getClusterMembers() {
        return null;
    }

    @SneakyThrows
    private ClusterConfig readClusterConfig() {
        YamlResourceLoader resourceLoader = YamlResourceLoader.getInstance();
        this.clusterConfig = resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterConfig.class);

        return clusterConfig;
    }
}
