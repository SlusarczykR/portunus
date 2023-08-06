package org.slusarczykr.portunus.cache.cluster.server.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.PortunusCluster;

import java.util.Collection;
import java.util.List;

public final class PortunusClusterStarter {

    private static final Logger log = LoggerFactory.getLogger(PortunusClusterStarter.class);

    public static void main(String[] args) {
        log.info("Starting portunus cluster");
        PortunusCluster portunusCluster = PortunusCluster.newInstance();

        Cache<String, String> cache = portunusCluster.getCache("testCache1");
        cache.put("testKey1", "testValue1");
        cache.put("testKey2", "testValue2");
        cache.put("testKey3", "testValue3");

        Collection<Cache.Entry<String, String>> cacheEntries = cache.getEntries(List.of("testKey1", "testKey2"));
        assert !cacheEntries.isEmpty();


    }
}
