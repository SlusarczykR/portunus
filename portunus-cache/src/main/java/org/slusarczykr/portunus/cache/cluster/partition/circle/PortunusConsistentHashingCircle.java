package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.partition.strategy.PartitionKeyStrategy;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PortunusConsistentHashingCircle implements PortunusHashingCircle, PartitionKeyStrategy {

    private static final Logger log = LoggerFactory.getLogger(PortunusConsistentHashingCircle.class);

    public static final int DEFAULT_NUMBER_OF_REPLICAS = 30;

    private final SortedMap<String, VirtualPortunusNode> circle = new ConcurrentSkipListMap<>();

    @Override
    public SortedMap<String, VirtualPortunusNode> get() {
        return new ConcurrentSkipListMap<>(circle);
    }

    @Override
    public boolean isEmpty() {
        return circle.isEmpty();
    }

    @Override
    public int getSize() {
        return circle.size();
    }

    @Override
    public Set<String> getKeys() {
        return circle.keySet();
    }

    @Override
    public Set<String> getAddresses() {
        Map<String, List<VirtualPortunusNode>> addressesByPhysicalNode = groupPortunusNodesByPhysicalNode();
        return addressesByPhysicalNode.keySet();
    }

    @Override
    public void update(SortedMap<String, VirtualPortunusNode> virtualPortunusNodes) {
        log.info("Start updating partition owner circle: {}", virtualPortunusNodes);
        circle.clear();
        circle.putAll(virtualPortunusNodes);
        log.info("Partition owner circle was updated");
        log.info("Partition owner circle: {}", circle);
    }

    private Map<String, List<VirtualPortunusNode>> groupPortunusNodesByPhysicalNode() {
        return circle.values().stream()
                .collect(Collectors.groupingBy(VirtualPortunusNode::getPhysicalNodeKey));
    }

    @Override
    public void add(Address address) throws PortunusException {
        PortunusNode node = new PortunusNode(address);
        int existingReplicas = getExistingReplicas(node);

        IntStream.range(0, DEFAULT_NUMBER_OF_REPLICAS).forEach(i -> {
            VirtualPortunusNode virtualNode = new VirtualPortunusNode(node, i + existingReplicas);
            String hashCode = generateHashCode(virtualNode.getKey());
            circle.put(hashCode, virtualNode);
        });
        log.info("Partition circle: {}", circle);
    }

    private int getExistingReplicas(PortunusNode node) {
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
    public String getServerAddress(Integer key) throws PortunusException {
        validateCircle();
        String nodeHashCode = getNodeHashCode(generateHashCode(String.valueOf(key)));
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

    public record VirtualPortunusNode(PortunusNode physicalNode, int replicaIndex) implements Node {

        @Override
        public String getKey() {
            return physicalNode.getKey() + "-" + replicaIndex;
        }

        public String getPhysicalNodeKey() {
            return physicalNode.getKey();
        }

        public boolean isVirtualNodeOf(PortunusNode node) {
            return getPhysicalNodeKey().equals(node.getKey());
        }

        public PortunusNode getPhysicalNode() {
            return physicalNode;
        }

        public int getReplicaIndex() {
            return replicaIndex;
        }
    }

    public record PortunusNode(Address address) implements Node {

        @Override
        public String getKey() {
            return String.format("%s:%s", address.hostname(), address.port());
        }
    }
}
