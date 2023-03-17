package org.slusarczykr.portunus.cache.cluster.partition.circle;

import org.apache.commons.codec.digest.DigestUtils;
import org.slusarczykr.portunus.cache.cluster.partition.strategy.PartitionKeyStrategy;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashingPartitionOwnerCircle implements PartitionOwnerCircle, PartitionKeyStrategy {

    private final SortedMap<String, String> circle = new ConcurrentSkipListMap<>();

    @Override
    public void add(String address) throws PortunusException {
        String hashCode = generateHashCode(address);

        if (circle.containsKey(hashCode)) {
            throw new PortunusException(String.format("Partition owner with address %s already exists", address));
        }
        circle.put(hashCode, address);
    }

    @Override
    public void remove(String address) throws PortunusException {
        String hashCode = generateHashCode(address);

        if (!circle.containsKey(hashCode)) {
            throw new PortunusException(String.format("Partition owner with address %s does not exists", address));
        }
        circle.remove(hashCode);
    }

    private String generateHashCode(String key) {
        return DigestUtils.sha256Hex(key);
    }

    @Override
    public String getServerAddress(String key) throws PortunusException {
        validateCircle();
        String hashCode = generateHashCode(key);

        if (!circle.containsKey(hashCode)) {
            hashCode = getFirstHashCode(hashCode);
        }
        return circle.get(hashCode);
    }

    private void validateCircle() throws PortunusException {
        if (circle.isEmpty()) {
            throw new PortunusException("Circle is empty");
        }
    }

    private String getFirstHashCode(String hashCode) {
        SortedMap<String, String> tailMap = circle.tailMap(hashCode);
        return tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }
}
