package org.slusarczykr.portunus.cache.cluster.config;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.util.List;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.DEFAULT_CONFIG_PATH;

public class DefaultClusterConfigService extends AbstractService implements ClusterConfigService {

    private static final DefaultClusterConfigService INSTANCE = new DefaultClusterConfigService();

    private ClusterConfig clusterConfig;

    public static DefaultClusterConfigService getInstance() {
        return INSTANCE;
    }

    private DefaultClusterConfigService() {
    }

    @Override
    public void onInitialization() throws PortunusException {
        this.clusterConfig = readClusterConfig();
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

    private ClusterConfig readClusterConfig() throws PortunusException {
        try {
            YamlResourceLoader resourceLoader = YamlResourceLoader.getInstance();
            return resourceLoader.load(DEFAULT_CONFIG_PATH, ClusterConfig.class);
        } catch (IOException e) {
            throw new PortunusException(String.format("Could not real cluster configuration from: '%s'", DEFAULT_CONFIG_PATH));
        }
    }

    @Override
    public String getName() {
        return ClusterConfigService.class.getSimpleName();
    }
}
