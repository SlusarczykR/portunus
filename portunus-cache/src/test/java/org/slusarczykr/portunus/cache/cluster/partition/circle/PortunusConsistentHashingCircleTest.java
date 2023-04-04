package org.slusarczykr.portunus.cache.cluster.partition.circle;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slusarczykr.portunus.cache.cluster.PortunusClusterInstance.DEFAULT_PORT;
import static org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.DEFAULT_NUMBER_OF_REPLICAS;

class PortunusConsistentHashingCircleTest {

    @Test
    void shouldAddNodesWithReplicasToCircle() {
        PortunusConsistentHashingCircle circle = new PortunusConsistentHashingCircle();
        List<Address> addresses = generateAddressesForHosts(3);
        addresses.forEach(it -> addAddress(circle, it));

        assertFalse(circle.isEmpty());
        assertEquals(addresses.size() * DEFAULT_NUMBER_OF_REPLICAS, circle.getSize());
        List<String> circleAddresses = new ArrayList<>(circle.getAddresses());
        assertEquals(addresses.size(), circleAddresses.size());
        assertTrue(circleAddresses.containsAll(Address.toPlainAddresses(addresses)));
    }

    @Test
    void shouldReturnOwnerNode() {
        PortunusConsistentHashingCircle circle = new PortunusConsistentHashingCircle();
        List<Address> addresses = generateAddressesForHosts(7);
        addresses.forEach(it -> addAddress(circle, it));

        assertFalse(circle.isEmpty());
        List<Integer> partitionIds = IntStream.rangeClosed(1, 1000).boxed().toList();
        Map<String, List<String>> partitionIdsByServerAddress = generatePartitionsForServerAddresses(circle, partitionIds);
        assertEquals(addresses.size(), partitionIdsByServerAddress.size());
        long targetNumberOfPartitions = round((float) partitionIds.size() / addresses.size());
        partitionIdsByServerAddress.forEach((owner, ownerPartitionIds) -> {
            long ownerNumberOfPartitionIds = round(ownerPartitionIds.size());
            assertEquals(targetNumberOfPartitions, ownerNumberOfPartitionIds);
        });
    }

    private long round(double number) {
        long i = (long) Math.ceil(number);
        return (i / 100) * 100;
    }

    ;

    private static Map<String, List<String>> generatePartitionsForServerAddresses(PortunusConsistentHashingCircle circle,
                                                                                  List<Integer> partitionIds) {
        return partitionIds.stream()
                .map(it -> getServerAddress(circle, it))
                .collect(Collectors.groupingBy(it -> it));
    }

    private static List<Address> generateAddressesForHosts(int numberOfRemoteHosts) {
        return IntStream.rangeClosed(1, numberOfRemoteHosts)
                .mapToObj(i -> new Address(String.format("host%s", i), DEFAULT_PORT))
                .toList();
    }

    @SneakyThrows
    private static void addAddress(PortunusConsistentHashingCircle circle, Address address) {
        circle.add(address);
    }

    @SneakyThrows
    private static String getServerAddress(PortunusConsistentHashingCircle circle, int partitionId) {
        return circle.getServerAddress(partitionId);
    }
}