package org.slusarczykr.portunus.cache.cluster.server;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.Serializable;

public interface PortunusServer {

    boolean isLocal();

    Address getAddress();

    String getPlainAddress();

    <K extends Serializable> boolean containsEntry(String cacheName, K key) throws PortunusException;

    <K, V> Cache<K, V> getCache(String name);

    record ClusterMemberContext(Address address) {

        public record Address(String hostname, int port) {

            @SneakyThrows
            public static Address from(String address) {
                String[] hostnameToPort = address.split(":");

                if (hostnameToPort.length != 2) {
                    throw new PortunusException("Invalid server address");
                }
                return new Address(hostnameToPort[0], Integer.parseInt(hostnameToPort[1]));
            }

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
