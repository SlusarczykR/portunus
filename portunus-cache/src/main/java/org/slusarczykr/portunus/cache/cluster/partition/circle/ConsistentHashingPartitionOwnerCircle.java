package org.slusarczykr.portunus.cache.cluster.partition.circle;

import com.google.common.hash.HashFunction;
import org.slusarczykr.portunus.cache.cluster.partition.strategy.PartitionKeyStrategy;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Collection;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashingPartitionOwnerCircle implements PartitionOwnerCircle, PartitionKeyStrategy {

    private final HashFunction hashFunction;
    private final SortedMap<Integer, String> circle = new ConcurrentSkipListMap<>();

    public ConsistentHashingPartitionOwnerCircle(HashFunction hashFunction, Collection<String> addresses) {
        this.hashFunction = hashFunction;
        addresses.forEach(this::add);
    }

    @Override
    public void add(String address) {
        int hashCode = generateHashCode(address);
        circle.put(hashCode, address);
    }

    @Override
    public void remove(String address) {
        int hashCode = generateHashCode(address);
        circle.remove(hashCode);
    }

    private int generateHashCode(String key) {
        return hashFunction.hashUnencodedChars(key).hashCode();
    }

    @Override
    public String getServerAddress(String key) throws PortunusException {
        if (circle.isEmpty()) {
            throw new PortunusException("Circle is empty");
        }
        int hashCode = generateHashCode(key);
        return getServerAddress(hashCode);
    }

    private String getServerAddress(int hashCode) {
        if (!circle.containsKey(hashCode)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hashCode);
            hashCode = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hashCode);
    }
}
