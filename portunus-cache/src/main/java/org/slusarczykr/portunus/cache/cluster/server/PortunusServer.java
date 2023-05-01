package org.slusarczykr.portunus.cache.cluster.server;

import lombok.SneakyThrows;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface PortunusServer {

    default boolean isLocal() {
        return false;
    }

    Address getAddress();

    String getPlainAddress();

    boolean anyEntry(String cacheName);

    <K extends Serializable> boolean containsEntry(String cacheName, K key);

    <K extends Serializable, V extends Serializable> Cache.Entry<K, V> getCacheEntry(String name, K key);

    <K extends Serializable, V extends Serializable> Set<Cache.Entry<K, V>> getCacheEntries(String name);

    <K extends Serializable, V extends Serializable> boolean put(String name, Cache.Entry<K, V> entry);

    <K extends Serializable, V extends Serializable> Cache.Entry<K, V> remove(String name, K key);

    void sendEvent(ClusterEvent event);

    record ClusterMemberContext(Address address, int numberOfServers) {

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

            public static List<String> toPlainAddresses(Collection<Address> addresses) {
                return addresses.stream()
                        .map(Address::toPlainAddress)
                        .toList();
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

    PaxosServer getPaxosServer();
}
