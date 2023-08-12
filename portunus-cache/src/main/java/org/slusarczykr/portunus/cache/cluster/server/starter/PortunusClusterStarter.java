package org.slusarczykr.portunus.cache.cluster.server.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;

public final class PortunusClusterStarter {

    private static final Logger log = LoggerFactory.getLogger(PortunusClusterStarter.class);

    public static void main(String[] args) {
        for (int i = 0; i < 10000; i++) {
            log.info("Starting portunus cluster: " + i);
            PortunusCluster portunusCluster = PortunusCluster.newInstance();
            portunusCluster.shutdown();
        }
    }
}
