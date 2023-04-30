package org.slusarczykr.portunus.cache.cluster.config;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.exception.PortunusException;
import org.slusarczykr.portunus.cache.util.resource.YamlResourceLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.*;

public class DefaultClusterConfigService extends AbstractService implements ClusterConfigService {

    private static final String PORTUNUS_PORT_PROPERTY_NAME = "portunusPort";
    private static final String PORTUNUS_MEMBERS_PROPERTY_NAME = "portunusMembers";

    private ClusterConfig clusterConfig;

    public static DefaultClusterConfigService newInstance(ClusterService clusterService) {
        return new DefaultClusterConfigService(clusterService);
    }

    private DefaultClusterConfigService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public void onInitialization() throws PortunusException {
        this.clusterConfig = Optional.ofNullable(clusterService.getClusterConfig())
                .orElseGet(this::readClusterConfig);
    }

    private ClusterConfig readClusterConfig() {
        ClusterConfig clusterConfig = readPropertyClusterConfig().orElseGet(this::readFileClusterConfig);
        clusterService.overrideClusterConfig(clusterConfig);

        return clusterConfig;
    }

    @Override
    public void overrideClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
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
    public boolean isMulticastEnabled() {
        return clusterConfig.isMulticast();
    }

    @Override
    public List<Address> getClusterMembers() {
        List<String> clusterMembers = Optional.ofNullable(clusterConfig.getMembers())
                .orElseGet(ArrayList::new);

        return clusterMembers.stream()
                .map(Address::from)
                .toList();
    }

    @Override
    public int getNumberOfClusterMembers() {
        return getClusterMembers().size();
    }

    private Optional<ClusterConfig> readPropertyClusterConfig() {
        return Optional.ofNullable(System.getProperty(PORTUNUS_PORT_PROPERTY_NAME))
                .map(it -> ClusterConfig.builder()
                        .port(Integer.parseInt(it))
                        .members(readPropertyClusterMembers())
                        .multicast(DEFAULT_MULTICAST_ENABLED)
                        .multicastPort(DEFAULT_MULTICAST_PORT)
                        .build());
    }

    private List<String> readPropertyClusterMembers() {
        return Optional.ofNullable(System.getProperty(PORTUNUS_MEMBERS_PROPERTY_NAME))
                .map(it -> List.of(it.split(",")))
                .orElseGet(ArrayList::new);
    }

    @SneakyThrows
    private ClusterConfig readFileClusterConfig() {
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
