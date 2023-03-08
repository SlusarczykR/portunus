package org.slusarczykr.portunus.cache.impl;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.event.CacheEventListener;
import org.slusarczykr.portunus.cache.event.CacheEventType;
import org.slusarczykr.portunus.cache.event.observer.DefaultCacheEntryObserver;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCache<K, V> implements Cache<K, V> {

    private final Map<K, Cache.Entry<K, V>> cache = new ConcurrentHashMap<>();
    private final DefaultCacheEntryObserver<K, V> cacheEntryObserver = new DefaultCacheEntryObserver<>();

    public DefaultCache(Map<CacheEventType, CacheEventListener> eventListeners) {
        eventListeners.forEach(cacheEntryObserver::register);
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean containsValue(V value) {
        return cache.entrySet().stream()
                .anyMatch(it -> it.getValue().equals(value));
    }

    public Optional<Cache.Entry<K, V>> getEntry(K key) {
        return Optional.ofNullable(cache.get(key))
                .map(it -> {
                    cacheEntryObserver.onAccess(it);
                    return it;
                });
    }

    public Collection<Cache.Entry<K, V>> getEntries(Collection<K> keys) {
        return keys.stream()
                .map(this::getEntry)
                .map(Optional::stream)
                .map(it -> (Cache.Entry<K, V>) it)
                .toList();

    }

    public Collection<Cache.Entry<K, V>> allEntries() {
        Collection<Cache.Entry<K, V>> entries = cache.values();
        entries.forEach(cacheEntryObserver::onAccess);

        return Collections.unmodifiableCollection(entries);
    }

    @Override
    public void put(K key, V value) {
        validate(key, value);
        Entry<K, V> entry = new Entry<>(key, value);
        cache.put(key, entry);
        cacheEntryObserver.onAdd(entry);
    }

    @Override
    public void putAll(Map<K, V> entries) {
        entries.forEach(this::validate);
        entries.forEach((key, value) -> {
            Entry<K, V> entry = new Entry<>(key, value);
            cacheEntryObserver.onAdd(entry);
            cache.put(key, entry);
        });
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
