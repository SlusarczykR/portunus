package org.slusarczykr.portunus.cache.cluster.server.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;

import java.util.Optional;

public final class PortunusClusterStarter {

    private static final String PORT_PROP = "port";

    private static final Logger log = LoggerFactory.getLogger(PortunusClusterStarter.class);

    public static void main(String[] args) {
        log.info("Starting portunus cluster");
        ClusterConfig clusterConfig = createClusterConfig();
        PortunusCluster cluster = PortunusCluster.newInstance(clusterConfig);
        System.exit(0);
    }

    private static ClusterConfig createClusterConfig() {
        return Optional.ofNullable(System.getProperty(PORT_PROP))
                .map(it -> ClusterConfig.builder()
                        .port(Integer.parseInt(it))
                        .build())
                .orElse(null);
    }
}
