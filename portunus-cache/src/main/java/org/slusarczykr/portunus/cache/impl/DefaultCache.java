package org.slusarczykr.portunus.cache.impl;

import org.slusarczykr.portunus.cache.Cache;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCache<K, V> implements Cache<K, V> {

    private final ConcurrentHashMap<K, V> cache = new ConcurrentHashMap<>();

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return Optional.ofNullable(cache.get(key))
                .map(it -> new Entry<>(key, it));
    }

    public Collection<Cache.Entry<K, V>> getEntries(Collection<K> keys) {
        return keys.stream()
                .map(this::getEntry)
                .map(Optional::stream)
                .map(it -> (Cache.Entry<K, V>) it)
                .toList();

    }

    public Collection<Cache.Entry<K, V>> allEntries() {
        return cache.entrySet().stream()
                .map(Entry::new)
                .map(it -> (Cache.Entry<K, V>) it)
                .toList();
    }

    @Override
    public void put(K key, V value) {
        validate(key, value);
        cache.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        cache.putAll(entries);
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
    }

    @Override
    public void removeAll(Collection<K> keys) {
        keys.forEach(cache::remove);
    }

    private void validate(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    public record Entry<K, V>(K key, V value) implements Cache.Entry<K, V> {

        public Entry(Map.Entry<K, V> entry) {
            this(entry.getKey(), entry.getValue());
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
