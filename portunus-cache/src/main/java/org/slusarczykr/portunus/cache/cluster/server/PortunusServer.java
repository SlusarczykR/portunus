package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

public interface PortunusServer {

    Address getAddress();

    String getPlainAddress();

    <K, V> Cache<K, V> getCache(String key);

    record ClusterMemberContext(Address address) {

        public record Address(String hostname, int port) {

            public String toPlainAddress() {
                return String.format("%s:%s", hostname, port);
            }
        }

        public String getHostname() {
            return address.hostname;
        }

        public int getPort() {
            return address.port;
        }

        public String getPlainAddress() {
            return address.toPlainAddress();
        }
    }
}
