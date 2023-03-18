package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.apache.commons.codec.digest.DigestUtils;
import org.slusarczykr.portunus.cache.cluster.partition.strategy.PartitionKeyStrategy;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;

public class PortunusConsistentHashingCircle implements PortunusHashingCircle, PartitionKeyStrategy {
    private static final int NUMBER_OF_REPLICAS = 3;

    private final SortedMap<String, VirtualPortunusNode> circle = new ConcurrentSkipListMap<>();

    @Override
    public void add(Address address) throws PortunusException {
        PortunusNode node = new PortunusNode(address);
        int existingReplicas = getExistingReplicas(node);

        IntStream.range(0, NUMBER_OF_REPLICAS).forEach(i -> {
            VirtualPortunusNode virtualNode = new VirtualPortunusNode(node, i + existingReplicas);
            String hashCode = generateHashCode(virtualNode.getKey());
            circle.put(hashCode, virtualNode);
        });
    }

    public int getExistingReplicas(PortunusNode node) {
        return (int) circle.values().stream()
                .filter(it -> it.isVirtualNodeOf(node))
                .count();
    }

    @Override
    public void remove(Address address) throws PortunusException {
        PortunusNode node = new PortunusNode(address);

        circle.entrySet().removeIf(it -> {
            VirtualPortunusNode virtualNode = it.getValue();
            return virtualNode.isVirtualNodeOf(node);
        });
    }

    private String generateHashCode(String key) {
        return DigestUtils.sha256Hex(key);
    }

    @Override
    public String getServerAddress(String key) throws PortunusException {
        validateCircle();
        String nodeHashCode = getNodeHashCode(generateHashCode(key));
        VirtualPortunusNode virtualNode = circle.get(nodeHashCode);

        return virtualNode.getPhysicalNodeKey();
    }

    private void validateCircle() throws PortunusException {
        if (circle.isEmpty()) {
            throw new PortunusException("Circle is empty");
        }
    }

    private String getNodeHashCode(String hashCode) {
        SortedMap<String, VirtualPortunusNode> tailMap = circle.tailMap(hashCode);
        return !tailMap.isEmpty() ? tailMap.firstKey() : circle.firstKey();
    }

    private interface Node {
        String getKey();
    }

    private record VirtualPortunusNode(PortunusNode physicalNode, int replicaIndex) implements Node {

        @Override
        public String getKey() {
            return physicalNode.getKey() + "-" + replicaIndex;
        }

        public String getPhysicalNodeKey() {
            return physicalNode.getKey();
        }

        public boolean isVirtualNodeOf(PortunusNode node) {
            return physicalNode.getKey().equals(node.getKey());
        }

        public PortunusNode getPhysicalNode() {
            return physicalNode;
        }
    }

    private record PortunusNode(Address address) implements Node {

        @Override
        public String getKey() {
            return String.format("%s:%s", address.hostname(), address.port());
        }
    }
}
