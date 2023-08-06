package org.slusarczykr.portunus.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Cache<K, V> {

    String getName();

    boolean isEmpty();

    boolean containsKey(K key);

    boolean containsValue(V value);

    Optional<Entry<K, V>> getEntry(K key);

    Collection<Entry<K, V>> getEntries(Collection<K> keys);

    Collection<Entry<K, V>> allEntries();

    void put(K key, V value);

    default void put(Entry<K, V> entry) {
        put(entry.getKey(), entry.getValue());
    }

    void putAll(Set<Cache.Entry<K, V>> entries);

    void putAll(Map<K, V> entries);

    Entry<K, V> remove(K key);

    Collection<Entry<K, V>> removeAll(Collection<K> keys);

    interface Entry<K, V> {

        K getKey();

        V getValue();
    }
}
